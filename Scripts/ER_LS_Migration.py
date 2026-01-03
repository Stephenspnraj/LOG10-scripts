import csv
import sys
import pymysql
import time
from logging import INFO, basicConfig, getLogger

logger = getLogger()
basicConfig(level=INFO, format="%(asctime)s %(message)s")


CSV_FILE = sys.argv[1]
MODE = sys.argv[2].upper()   # DRYRUN / UPDATE

BATCH_SIZE = 1000          # Very safe for InnoDB
SLEEP_TIME = 0.01         # Let purge keep up
MAX_BATCHES_PER_ROW = 1000
OUTPUT_FILE = "bulk_location_update_output.csv"
MANIFEST_DETAILS_FILE = "updated_manifests_details.csv"

def get_conn():
    return pymysql.connect(
        host="log10-replica-replica.cco3osxqlq4g.ap-south-1.rds.amazonaws.com",
        port=3306,
        user="log10_scripts",
        password="D2lx7Wz0Wm9ISAY-Vp1gxKDTzLRRl5k1m",
        database="loadshare",
         autocommit=False,
         cursorclass=pymysql.cursors.DictCursor
    )
# ---------------- DB ----------------

def get_location_ids(conn, old_loc, new_loc):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT client_location_name, id
            FROM locations
            WHERE status = 1
              AND client_location_name IN (%s, %s)
        """, (old_loc, new_loc))
        rows = cur.fetchall()

    if len(rows) != 2:
        raise Exception("Old or New location missing")

    old_id = next(r["id"] for r in rows if r["client_location_name"] == old_loc)
    new_id = next(r["id"] for r in rows if r["client_location_name"] == new_loc)
    return old_id, new_id

def fetch_manifest_batch(conn, old_id):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT *
            FROM manifests
            WHERE updated_at > CURRENT_DATE - INTERVAL 2 DAY
              AND next_loc_id = %s 
              AND is_active = 1
              AND manifest_status NOT IN ('completed')
            ORDER BY id
            LIMIT %s
        """, (old_id, BATCH_SIZE))
        return cur.fetchall()

def update_manifest_batch(conn, ids, new_id):
    if not ids:
        return 0
    with conn.cursor() as cur:
        placeholders = ",".join(ids)
        cur.execute(
            f"UPDATE manifests SET next_loc_id = %s WHERE id IN ({placeholders})",
            (new_id,)
        )
        return cur.rowcount

# ---------------- CORE ----------------
def process_row(old_loc, new_loc, summary_writer, manifest_writer):
    logger.info(f"\nProcessing {old_loc} ➜ {new_loc}")
    conn = get_conn()

    try:
        old_id, new_id = get_location_ids(conn, old_loc, new_loc)
        total_updated = 0
        batch_no = 0

        while batch_no < MAX_BATCHES_PER_ROW:
            rows = fetch_manifest_batch(conn, old_id)
            if not rows:
                break

            ids = [str(r["id"]) for r in rows]

            # Write FULL manifest details
            for r in rows:
                manifest_writer.writerow([
                    old_loc,
                    new_loc,
                    r.get("id"),
                    r.get("manifest_code"),
                    old_id,
                    new_id,
                    r.get("manifest_status"),
                    r.get("updated_at")
                ])

            updated = update_manifest_batch(conn, ids, new_id)
            conn.commit()
            total_updated += updated
            batch_no += 1

            logger.info(f"   ➤ Batch {batch_no}: Updated {updated} manifests")
            time.sleep(SLEEP_TIME)

        summary_writer.writerow([
            old_loc,
            new_loc,
            old_id,
            new_id,
            total_updated,
            "SUCCESS"
        ])

        logger.info(f"   ✅ Done | Total Updated: {total_updated}")

    except Exception as e:
        summary_writer.writerow([
            old_loc,
            new_loc,
            "",
            "",
            "",
            f"FAILED: {e}"
        ])
        logger.info(f"   ❌ Failed: {e}")

    finally:
        conn.close()

# ---------------- MAIN ----------------
def main():
    with open(OUTPUT_FILE, "w", newline="") as summary_file, \
         open(MANIFEST_DETAILS_FILE, "w", newline="") as manifest_file:

        summary_writer = csv.writer(summary_file)
        manifest_writer = csv.writer(manifest_file)

        summary_writer.writerow([
            "old_loc", "new_loc",
            "old_loc_id", "new_loc_id",
            "manifests_updated", "status"
        ])

        manifest_writer.writerow([
            "old_loc", "new_loc",
            "manifest_id", "manifest_code",
            "old_destination_loc_id", "new_destination_loc_id",
            "manifest_status", "updated_at"
        ])

        with open(CSV_FILE) as f:
            reader = csv.DictReader(f)
            for row in reader:
                # 🔑 One CSV row → full commit → next row
                process_row(
                    row["old_loc"].strip(),
                    row["new_loc"].strip(),
                    summary_writer,
                    manifest_writer
                )

    logger.info(f"\n📁 Summary Output: {OUTPUT_FILE}")
    logger.info(f"📁 Manifest Details Output: {MANIFEST_DETAILS_FILE}")

if __name__ == "__main__":
    main()

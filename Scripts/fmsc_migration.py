import os
import pandas as pd
import pymysql
import re
from tabulate import tabulate
from datetime import datetime
import time

from logging import INFO, basicConfig, getLogger

logger = getLogger()
basicConfig(level=INFO, format="%(asctime)s %(message)s")

# ------------------------------
# CONFIG
# ------------------------------
INPUT_CSV_PATH = "fmsc_migration.csv"  
OUTPUT_XLSX_PATH = "fmsc_migration_output.xlsx" 

DB_CONFIG = {
    "host": "log10-replica-replica.cco3osxqlq4g.ap-south-1.rds.amazonaws.com",
    "user": "log10_scripts",
    "password": "D2lx7Wz0Wm9ISAY-Vp1gxKDTzLRRl5k1m",
    "database": "loadshare",
    "port": 3306
}

# ------------------------------
# DB Connection
# ------------------------------
def get_connection():
    return pymysql.connect(**DB_CONFIG, cursorclass=pymysql.cursors.DictCursor)

# ------------------------------
# Helpers
# ------------------------------
def sanitize_string(value):
    """Remove illegal characters for Excel compatibility."""
    if not isinstance(value, str):
        return value
    # Remove control characters (ASCII 0-31 except 9, 10, 13)
    return re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F]', '', value)
    
def normalize_alias(alias):
    return f"{alias}.FMSC" if alias and not alias.upper().endswith(".FMSC") else alias

def location_exists(conn, alias):
    base_alias = alias.split('.')[0]
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM locations WHERE alias = %s", (base_alias,))
        return cur.fetchone() is not None

def show_existing_network_metadata(conn, source, destination):
    base_dest = destination.split('.')[0]
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, location_alias, next_location_alias, crossdock_alias, is_active, updated_at
            FROM network_metadata
            WHERE location_alias = %s
              AND (next_location_alias = %s OR next_location_alias = %s)
              AND is_active = 1
        """, (source, f"{base_dest}.FMSC", base_dest))
        rows = cur.fetchall()
    if rows:
        logger.info(f"\n📌 Existing ACTIVE mappings for {source} → {destination}:")
        logger.info(tabulate(rows, headers="keys", tablefmt="pretty"))
    else:
        logger.info(f"(No active mapping found for {source} → {destination})")
    return rows

def deactivate_mapping(conn, source, destination):
    existing = show_existing_network_metadata(conn, source, destination)
    if not existing:
        return 0
    logger.info(f"🔻 Deactivating mapping for {source} → {destination}")
    base_dest = destination.split('.')[0]
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE network_metadata
               SET is_active = 0,
                   audit_log = 'Deactivated by FMSC Migration Script'
             WHERE location_alias = %s
               AND (next_location_alias = %s OR next_location_alias = %s)
               AND is_active = 1
        """, (source, f"{base_dest}.FMSC", base_dest))
        count = cur.rowcount
    conn.commit()
    logger.info(f"✅ Deactivated {count} record(s).\n")
    return count

def activate_or_insert_mapping(conn, source, destination, crossdock):
    base_dest = destination.split('.')[0]
    base_cross = crossdock.split('.')[0] if crossdock else None
    logger.info(f"🔹 Activating or Inserting mapping: {source} → {destination} (crossdock={crossdock})")
    with conn.cursor() as cur:
        if crossdock:
            cur.execute("""
                SELECT id FROM network_metadata
                 WHERE location_alias = %s
                   AND (next_location_alias = %s OR next_location_alias = %s)
                   AND (crossdock_alias = %s OR crossdock_alias = %s)
            """, (source, f"{base_dest}.FMSC", base_dest, f"{base_cross}.FMSC", base_cross))
        else:
            cur.execute("""
                SELECT id FROM network_metadata
                 WHERE location_alias = %s
                   AND (next_location_alias = %s OR next_location_alias = %s)
                   AND (crossdock_alias IS NULL OR crossdock_alias = '')
            """, (source, f"{base_dest}.FMSC", base_dest))
        existing = cur.fetchone()
        if existing:
            logger.info(f"Found existing mapping ID {existing['id']} → Activating")

            # 🔍 Fetch and print the existing record details in table format
            cur.execute("""
                SELECT id, location_alias, next_location_alias, crossdock_alias, is_active, updated_at
                FROM network_metadata
                WHERE id = %s
            """, (existing["id"],))
            existing_record_details = cur.fetchall()
            if existing_record_details:
                logger.info("\n📄 Existing record details before activation:")
                logger.info(tabulate(existing_record_details, headers="keys", tablefmt="pretty"))

            # Now activate it
            cur.execute("""
                UPDATE network_metadata
                SET is_active = 1,
                    audit_log = 'Activated by FMSC Migration Script'
                WHERE id = %s
            """, (existing["id"],))
            action = "Activated"

        else:
            logger.info("No existing mapping → Inserting new one")
            if crossdock:
                cur.execute("""
                    INSERT INTO network_metadata (location_alias, next_location_alias, crossdock_alias, is_active, audit_log)
                    VALUES (%s, %s, %s, 1, 'Activated by FMSC Migration Script')
                """, (source, f"{base_dest}.FMSC", f"{base_cross}.FMSC"))
            else:
                cur.execute("""
                    INSERT INTO network_metadata (location_alias, next_location_alias, is_active, audit_log)
                    VALUES (%s, %s, 1, 'Activated by FMSC Migration Script')
                """, (source, f"{base_dest}.FMSC"))
            action = "Inserted"
    conn.commit()
    logger.info(f"✅ {action}\n")
    return action

all_pending_manifests = []  # for CSV export

# def show_pending_manifests(conn, current_alias, destination_alias, next_alias=None):
#     base_current = current_alias.split('.')[0]
#     base_dest = destination_alias.split('.')[0]
#     with conn.cursor() as cur:
#         cur.execute("""
#             SELECT m.manifest_code,
#                    cl.alias AS current_loc,
#                    dl.alias AS destination_loc,
#                    nl.alias AS next_loc,
#                    m.updated_at
#             FROM manifests m
#             JOIN locations cl ON cl.id = m.current_loc_id
#             JOIN locations dl ON dl.id = m.destination_loc_id
#             JOIN locations nl ON nl.id = m.next_loc_id
#            WHERE cl.alias = %s
#              AND dl.alias = %s
#              AND m.manifest_status = 'PENDING'
#              AND m.is_active = 1
#              AND m.updated_at > NOW() - INTERVAL 30 DAY
#         """, (base_current, base_dest))
#         rows = cur.fetchall()
#     logger.info(f"📦 Pending manifests for {current_alias} → {destination_alias}: {len(rows)} found")
#     if rows:
#         logger.info(tabulate(rows, headers="keys", tablefmt="pretty"))
#         for r in rows:
#             r["current_alias"] = current_alias
#             r["destination_alias"] = destination_alias
#             r["expected_next_loc"] = next_alias
#             all_pending_manifests.append(r)
#     else:
#         logger.info("(No pending manifests found)")
#     return rows

def show_pending_manifests(conn, current_alias, destination_alias, next_alias=None):
    base_current = current_alias.split('.')[0]
    base_dest = destination_alias.split('.')[0]
    with conn.cursor() as cur:
        cur.execute("""
            SELECT m.manifest_code,
                   cl.alias AS current_loc,
                   dl.alias AS destination_loc,
                   nl.alias AS next_loc,
                   m.updated_at
            FROM manifests m
            JOIN locations cl ON cl.id = m.current_loc_id
            JOIN locations dl ON dl.id = m.destination_loc_id
            JOIN locations nl ON nl.id = m.next_loc_id
           WHERE cl.alias = %s
             AND dl.alias = %s
             AND m.manifest_status = 'PENDING'
             AND m.is_active = 1
             AND m.updated_at > NOW() - INTERVAL 30 DAY
        """, (base_current, base_dest))
        rows = cur.fetchall()
    logger.info(f"📦 Pending manifests for {current_alias} → {destination_alias}: {len(rows)} found")
    if rows:
        logger.info(tabulate(rows, headers="keys", tablefmt="pretty"))
        for r in rows:
            # Create a copy of the row for Excel output with sanitized manifest_code
            r_cleaned = r.copy()
            r_cleaned['manifest_code'] = sanitize_string(r['manifest_code'])  # Sanitize only for Excel
            if r['manifest_code'] != r_cleaned['manifest_code']:
                logger.info(f"⚠ Sanitized manifest_code for Excel: {r['manifest_code']} → {r_cleaned['manifest_code']}")
            r_cleaned["current_alias"] = current_alias
            r_cleaned["destination_alias"] = destination_alias
            r_cleaned["expected_next_loc"] = next_alias
            all_pending_manifests.append(r_cleaned)
    else:
        logger.info("(No pending manifests found)")
    return rows
    
def update_pending_manifests(conn, current_alias, destination_alias, next_alias):
    pending = show_pending_manifests(conn, current_alias, destination_alias, next_alias)
    if not pending:
        return 0
    def get_location_id(alias):
        base_alias = alias.split('.')[0] if alias else alias
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM locations WHERE alias = %s", (base_alias,))
            res = cur.fetchone()
            return res["id"] if res else None
    current_id = get_location_id(current_alias)
    destination_id = get_location_id(destination_alias)
    next_id = get_location_id(next_alias)
    if not current_id or not destination_id or not next_id:
        logger.info("⚠ One or more location IDs not found → Skipping update.")
        return 0
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE manifests
               SET next_loc_id = %s
             WHERE current_loc_id = %s
               AND destination_loc_id = %s
               AND manifest_status = 'PENDING'
               AND is_active = 1
               AND updated_at > NOW() - INTERVAL 30 DAY
        """, (next_id, current_id, destination_id))
        count = cur.rowcount
    conn.commit()
    logger.info(f"✅ Updated {count} manifests\n")
    return count

# ------------------------------
# Main
# ------------------------------
def main():
    if not os.path.exists(INPUT_CSV_PATH):
        logger.info(f"❌ CSV file not found: {INPUT_CSV_PATH}")
        return
    df = pd.read_csv(INPUT_CSV_PATH)
    required_cols = {"FMH", "FMCD", "FMSC"}
    df_cols_upper = {c.upper(): c for c in df.columns}
    if not required_cols.issubset(df_cols_upper.keys()):
        logger.info("❌ Missing required columns in CSV")
        return
    conn = get_connection()
    summary_rows = []
    for i, row in df.iterrows():
        logger.info("\n" + "="*80)
        logger.info(f"📍 Processing Row {i+1}")
        logger.info(row.to_dict())  # full row data
        logger.info("-"*80)
        FMH = str(row[df_cols_upper["FMH"]]).strip()
        FMCD = str(row[df_cols_upper["FMCD"]]).strip() if pd.notna(row[df_cols_upper["FMCD"]]) else ""
        FMSC = str(row[df_cols_upper["FMSC"]]).strip()
        FMSC_db = normalize_alias(FMSC)
        FMCD_db = normalize_alias(FMCD) if FMCD else None
        do_manifest_correction = str(row.get("is_manifest_correction_required", "")).strip().lower() in ("1","true","yes","y")
        missing = [loc for loc in [FMH,FMSC]+([FMCD] if FMCD else []) if loc and not location_exists(conn, loc)]
        if missing:
            logger.info(f"⚠ Skipping row due to missing locations: {missing}")
            summary_rows.append({"Row":i+1,"FMH":FMH,"FMCD":FMCD or "-","FMSC":FMSC,"ManifestCorrection":"N/A","Actions":f"Skipped: Missing locations : {', '.join(missing)}"})
            continue
        actions = []
        actions.append(f"Deactivated({deactivate_mapping(conn, FMH, FMSC_db)})")
        actions.append(activate_or_insert_mapping(conn, FMH, FMSC_db, FMCD_db if FMCD else None))
        if do_manifest_correction:
            if FMCD:
                fw = update_pending_manifests(conn, FMH, FMSC, FMCD)
                rto = update_pending_manifests(conn, FMSC, FMH, FMCD)
            else:
                fw = update_pending_manifests(conn, FMH, FMSC, FMSC)
                rto = update_pending_manifests(conn, FMSC, FMH, FMH)
            actions.append(f"ManifestUpdated(FWD:{fw},RTO:{rto})")
        else:
            logger.info("(Manifest correction skipped)")
            actions.append("ManifestCorrectionSkipped")
        summary_rows.append({"Row":i+1,"FMH":FMH,"FMCD":FMCD or "-","FMSC":FMSC,"ManifestCorrection":"Yes" if do_manifest_correction else "No","Actions":", ".join(actions)})
        print("Sleeping for 5 seconds")
        time.sleep(5)
    conn.close()
    # Save one Excel with two sheets
    logger.info("\n=== SUMMARY REPORT ===")
    logger.info(tabulate(summary_rows, headers="keys", tablefmt="pretty"))
    with pd.ExcelWriter(OUTPUT_XLSX_PATH) as writer:
        pd.DataFrame(summary_rows).to_excel(writer, sheet_name="Summary", index=False)
        if all_pending_manifests:
            pd.DataFrame(all_pending_manifests).to_excel(writer, sheet_name="Manifests", index=False)
        else:
            pd.DataFrame(columns=["No pending manifests"]).to_excel(writer, sheet_name="Manifests", index=False)
    logger.info(f"\n✅ Output saved to: {OUTPUT_XLSX_PATH}")

if __name__ == "__main__":
    main()

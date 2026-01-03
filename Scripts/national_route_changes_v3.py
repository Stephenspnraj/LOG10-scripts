import os
import pymysql
import pandas as pd
from tabulate import tabulate
import time

from logging import INFO, basicConfig, getLogger

logger = getLogger()
basicConfig(level=INFO, format="%(asctime)s %(message)s")

# ---------- CONFIG ----------
INPUT_CSV_PATH = "national_route_changes_input.csv"
OUTPUT_XLSX_PATH = "national_route_changes_output.xlsx"

DB_CONFIG = {
    "host": "log10-replica-replica.cco3osxqlq4g.ap-south-1.rds.amazonaws.com",
    "user": "log10_scripts",
    "password": "D2lx7Wz0Wm9ISAY-Vp1gxKDTzLRRl5k1m",
    "database": "loadshare",
    "port": 3306,
    "charset": 'utf8mb4'
}

def get_connection():
    return pymysql.connect(**DB_CONFIG, cursorclass=pymysql.cursors.DictCursor)

def normalize_alias(alias, suffix):
    if alias and not alias.upper().endswith(f".{suffix}"):
        return f"{alias}.{suffix}"
    return alias

def base(alias):
    return alias.split('.')[0] if alias else alias

def location_exists(conn, alias):
    if not alias:
        return False
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM locations WHERE alias = %s", (base(alias),))
        return cur.fetchone() is not None

def show_existing_network_metadata(conn, source, destination):
    base_source = base(source)
    base_dest   = base(destination)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, location_alias, next_location_alias, crossdock_alias, is_active, updated_at
            FROM network_metadata
            WHERE SUBSTRING_INDEX(location_alias, '.', 1) = %s
              AND SUBSTRING_INDEX(next_location_alias, '.', 1) = %s
              AND is_active = 1
        """, (base_source, base_dest))
        rows = cur.fetchall()
    if rows:
        print(tabulate(rows, headers="keys", tablefmt="pretty"))
    else:
        print("(No active mapping found)")
    return rows

def deactivate_mapping(conn, source, destination):
    logger.info(f"\n--- Checking ACTIVE mappings before deactivation for {source} → {destination} ---")
    # Deactivate forward mapping
    existing = show_existing_network_metadata(conn, source, destination)
    count = 0
    if existing:
        base_source = base(source)
        base_dest   = base(destination)
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE network_metadata
                   SET is_active = 0,
                       audit_log = CONCAT('Deactivated by National Route Script: ', %s, ' → ', %s)
                 WHERE SUBSTRING_INDEX(location_alias, '.', 1) = %s
                   AND SUBSTRING_INDEX(next_location_alias, '.', 1) = %s
                   AND is_active = 1
            """, (source, destination, base_source, base_dest))
            count = cur.rowcount
        conn.commit()
        logger.info(f"✅ Deactivated {count} mapping(s) for {source} → {destination}")
    # else: (print already handled in show_existing_network_metadata)

    # Deactivate reverse mapping
    logger.info(f"\n--- Checking ACTIVE mappings before deactivation for {destination} → {source} (reverse) ---")
    reverse_existing = show_existing_network_metadata(conn, destination, source)
    reverse_count = 0
    if reverse_existing:
        base_rev_source = base(destination)
        base_rev_dest   = base(source)
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE network_metadata
                   SET is_active = 0,
                       audit_log = CONCAT('Deactivated by National Route Script (reverse): ', %s, ' → ', %s)
                 WHERE SUBSTRING_INDEX(location_alias, '.', 1) = %s
                   AND SUBSTRING_INDEX(next_location_alias, '.', 1) = %s
                   AND is_active = 1
            """, (destination, source, base_rev_source, base_rev_dest))
            reverse_count = cur.rowcount
        conn.commit()
        logger.info(f"✅ Deactivated {reverse_count} mapping(s) for {destination} → {source} (reverse)")
    # else: (print already handled in show_existing_network_metadata)

    return count + reverse_count

def activate_or_insert_mapping(conn, source, destination, crossdock=None):

    # Always insert a new mapping (after deactivation step in main logic)
    with conn.cursor() as cur:
        if crossdock:
            cur.execute("""
                INSERT INTO network_metadata 
                    (location_alias, next_location_alias, crossdock_alias, is_active, audit_log, updated_at)
                VALUES (%s, %s, %s, 1, CONCAT('Inserted by National Route Script: ', %s, ' → ', %s), NOW())
            """, (source, destination, crossdock, source, destination))
        else:
            cur.execute("""
                INSERT INTO network_metadata 
                    (location_alias, next_location_alias, is_active, audit_log, updated_at)
                VALUES (%s, %s, 1, CONCAT('Inserted by National Route Script: ', %s, ' → ', %s), NOW())
            """, (source, destination, source, destination))
        action = "Inserted"

    conn.commit()
    logger.info(f"✅ {action} mapping for {source} → {destination}")
    logger.info(f"--- Checking ACTIVE mappings after {action} for {source} → {destination} ---")
    show_existing_network_metadata(conn, source, destination)
    return action


all_pending_manifests = []

def show_pending_manifests(conn, current_alias, destination_alias, next_alias=None):
    rows = []
    for days_ago in range(0, 21, 7):
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
                 AND m.updated_at > (NOW() - INTERVAL %s DAY)
                 AND m.updated_at <= (NOW() - INTERVAL %s DAY)
            """, (base(current_alias), base(destination_alias), days_ago, days_ago - 7 if days_ago > 0 else 0))
            rows.extend(cur.fetchall())
    # Only keep manifests where next_loc != next_alias
    to_update = [r for r in rows if str(r['next_loc']) != str(base(next_alias))]
    logger.info(f"Pending manifests for {current_alias} → {destination_alias}: {len(to_update)} need update")
    if to_update:
        print(tabulate(to_update, headers="keys", tablefmt="pretty"))
        for r in to_update:
            r["current_alias"] = current_alias
            r["destination_alias"] = destination_alias
            r["expected_next_loc"] = next_alias
            all_pending_manifests.append(r)
    return to_update

def get_location_id(conn, alias):
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM locations WHERE alias = %s", (base(alias),))
        res = cur.fetchone()
        return res["id"] if res else None

def update_pending_manifests(conn, current_alias, destination_alias, next_alias):
    pending = show_pending_manifests(conn, current_alias, destination_alias, next_alias)
    if not pending:
        return 0
    current_id = get_location_id(conn, current_alias)
    destination_id = get_location_id(conn, destination_alias)
    next_id = get_location_id(conn, next_alias)
    if not current_id or not destination_id or not next_id:
        logger.info("⚠ Missing location IDs — skipping update")
        return 0
    # Only update manifests where next_loc != next_alias
    to_update_codes = [r['manifest_code'] for r in pending if str(r['next_loc']) != str(base(next_alias))]
    if not to_update_codes:
        logger.info(f"No manifests need update for {current_alias} → {destination_alias} → {next_alias}")
        return 0
    with conn.cursor() as cur:
        # Use manifest_code to update only those that need it
        format_strings = ','.join(['%s'] * len(to_update_codes))
        cur.execute(f"""
            UPDATE manifests
               SET next_loc_id = %s
             WHERE current_loc_id = %s
               AND destination_loc_id = %s
               AND manifest_status = 'PENDING'
               AND is_active = 1
               AND updated_at > NOW() - INTERVAL 21 DAY
               AND manifest_code IN ({format_strings})
        """, (next_id, current_id, destination_id, *to_update_codes))
        count = cur.rowcount
    conn.commit()
    logger.info(f"Updated {count} manifests for {current_alias} → {destination_alias} → {next_alias}")
    return count

def main():
    if not os.path.exists(INPUT_CSV_PATH):
        logger.info(f"❌ Missing file {INPUT_CSV_PATH}")
        return
    df = pd.read_csv(INPUT_CSV_PATH)
    cols = {c.lower(): c for c in df.columns}
    conn = get_connection()
    summary_rows = []

    for i, row in df.iterrows():
        print("\n" + "="*80)
        logger.info(f"📍 Processing Row {i+1}: {dict(row)}")
        print("="*80)

        FMSC = str(row[cols['fmsc']]).strip()
        FMCD = str(row[cols['fmcd']]).strip() if pd.notna(row[cols['fmcd']]) else ""
        LMCD = str(row[cols['lmcd']]).strip() if pd.notna(row[cols['lmcd']]) else ""
        LMSC = str(row[cols['lmsc']]).strip()

        location_alias = normalize_alias(FMSC, "FMSC")
        next_location_alias = normalize_alias(LMSC, "LMSC")
        needs_correction = str(row.get(cols.get('is_manifest_correction_required',''),"")).strip().lower() in ("1","1.0","true","yes","y")

        # Validation with raw values
        raw_to_norm = {FMSC: location_alias, LMSC: next_location_alias}
        if FMCD:
            raw_to_norm[FMCD] = normalize_alias(FMCD, "FMSC")
        if LMCD:
            raw_to_norm[LMCD] = normalize_alias(LMCD, "LMSC")

        missing_raw = [raw for raw, norm in raw_to_norm.items() if not location_exists(conn, norm)]
        if missing_raw:
            logger.info(f"⚠ Skipping Row {i+1} - Missing locations: {', '.join(missing_raw)}")
            summary_rows.append({
                "Row": i+1, "FMSC": FMSC, "FMCD": FMCD or "-", "LMCD": LMCD or "-",
                "LMSC": LMSC, "ManifestCorrection": "N/A",
                "Actions": f"Skipped - Missing locations: {', '.join(missing_raw)}"
            })
            continue

        actions = []
        if not FMCD and not LMCD:  # Case 1
            deactivate_mapping(conn, location_alias, next_location_alias)
            activate_or_insert_mapping(conn, location_alias, next_location_alias, None)
            if needs_correction:
                actions.append(f"ManifestUpdated(FWD:{update_pending_manifests(conn, location_alias, next_location_alias, next_location_alias)},"
                               f"RTO:{update_pending_manifests(conn, next_location_alias, location_alias, location_alias)})")
            else:
                actions.append("ManifestCorrectionSkipped")
        elif FMCD and not LMCD:  # Case 2
            crossdock_alias = normalize_alias(FMCD, "FMSC")
            deactivate_mapping(conn, location_alias, next_location_alias)
            activate_or_insert_mapping(conn, location_alias, next_location_alias, crossdock_alias)
            if needs_correction:
                actions.append(f"ManifestUpdated(FWD:{update_pending_manifests(conn, location_alias, next_location_alias, crossdock_alias)},"
                               f"RTO:{update_pending_manifests(conn, next_location_alias, location_alias, crossdock_alias)})")
            else:
                actions.append("ManifestCorrectionSkipped")
        elif not FMCD and LMCD:  # Case 3
            crossdock_alias = normalize_alias(LMCD, "LMSC")
            deactivate_mapping(conn, location_alias, next_location_alias)
            activate_or_insert_mapping(conn, location_alias, next_location_alias, crossdock_alias)
            if needs_correction:
                actions.append(f"ManifestUpdated(FWD:{update_pending_manifests(conn, location_alias, next_location_alias, crossdock_alias)},"
                               f"RTO:{update_pending_manifests(conn, next_location_alias, location_alias, crossdock_alias)})")
            else:
                actions.append("ManifestCorrectionSkipped")
        elif FMCD and LMCD:  # Case 4
            alias1 = location_alias
            cross1 = normalize_alias(FMCD, "FMSC")
            alias2 = cross1
            cross2 = normalize_alias(LMCD, "LMSC")
            deactivate_mapping(conn, alias1, next_location_alias)
            activate_or_insert_mapping(conn, alias1, next_location_alias, cross1)
            deactivate_mapping(conn, alias2, next_location_alias)
            activate_or_insert_mapping(conn, alias2, next_location_alias, cross2)
            if needs_correction:
                fw1 = update_pending_manifests(conn, alias1, next_location_alias, cross1)
                rto1 = update_pending_manifests(conn, next_location_alias, alias1, cross2)
                fw2 = update_pending_manifests(conn, alias2, next_location_alias, cross2)
                rto2 = update_pending_manifests(conn, next_location_alias, alias2, cross2)
                actions.append(f"ManifestUpdated(FWD1:{fw1},RTO1:{rto1},FWD2:{fw2},RTO2:{rto2})")
            else:
                actions.append("ManifestCorrectionSkipped")

        summary_rows.append({
            "Row": i+1, "FMSC": FMSC, "FMCD": FMCD or "-", "LMCD": LMCD or "-",
            "LMSC": LMSC, "ManifestCorrection": "Yes" if needs_correction else "No",
            "Actions": ", ".join(actions)
        })

        logger.info("Sleeping 1 seconds")

        time.sleep(1)

    conn.close()
    print("\n=== SUMMARY ===")
    print(tabulate(summary_rows, headers="keys", tablefmt="pretty"))
    try:
        with pd.ExcelWriter(OUTPUT_XLSX_PATH, engine="openpyxl") as writer:
            pd.DataFrame(summary_rows).to_excel(writer, sheet_name="Summary", index=False)
            if all_pending_manifests:
                # Convert all columns to string and replace problematic characters
                df_manifests = pd.DataFrame(all_pending_manifests).astype(str)
                df_manifests = df_manifests.map(lambda x: x.encode('utf-8', errors='replace').decode('utf-8', errors='replace'))
                df_manifests.to_excel(writer, sheet_name="Manifests", index=False)
            else:
                pd.DataFrame(columns=["No pending manifests"]).to_excel(writer, sheet_name="Manifests", index=False)
        logger.info(f"\n✅ Output saved to {OUTPUT_XLSX_PATH}")
    except Exception as e:
        logger.error(f"❌ Failed to write Excel output: {e}")

if __name__ == "__main__":
    main()

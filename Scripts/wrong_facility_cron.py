import pymysql
import pandas as pd
from datetime import datetime
import logging
from logging.handlers import RotatingFileHandler
from tabulate import tabulate  # ✅ for summary table

# ===== Database connection details =====
mysql_host = 'log10-replica-replica.cco3osxqlq4g.ap-south-1.rds.amazonaws.com'
mysql_port = 3306
mysql_user = 'log10_scripts'
mysql_password = 'D2lx7Wz0Wm9ISAY-Vp1gxKDTzLRRl5k1m'
mysql_db = 'loadshare'

# ===== Logging Setup =====
log_file = 'misroute_configs_log.txt'

logger = logging.getLogger("misroute_logger")
logger.setLevel(logging.INFO)
handler = RotatingFileHandler(log_file, maxBytes=5*1024*1024, backupCount=3)  # 5MB, keep 3 backups
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

def log(msg):
    print(msg)
    logger.info(msg)

# ===== Main processing =====
try:
    with pymysql.connect(
        host=mysql_host,
        port=mysql_port,
        user=mysql_user,
        password=mysql_password,
        db=mysql_db,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True
    ) as conn:

        with conn.cursor() as cursor:
            # Fetch valid LMSC aliases
            sc_query = """
            SELECT distinct l2.alias AS location_name
            FROM locations l2 
            JOIN partners p ON l2.entity_id = p.id 
            JOIN network_metadata nm2 ON nm2.location_alias = CONCAT(l2.alias, '.LMSC')
            WHERE l2.status = 1 
              AND l2.location_ops_type = 'SC' 
              AND p.name NOT LIKE '%virtual%' 
              AND p.name NOT LIKE '%test%' 
              AND p.name NOT LIKE '%off%'
              AND nm2.is_active = 1
            ORDER BY l2.id DESC
            """
            cursor.execute(sc_query)
            sc_result = cursor.fetchall()
            SortCentre_alias = [row['location_name'] for row in sc_result]

        log(f"Valid Last Mile Sort Centers: {', '.join(SortCentre_alias)}\n")

        if not SortCentre_alias:
            log("No valid Last Mile Sort Centers found. Exiting...")
            exit()

        # ✅ summary collector
        summary_data = []

        for SC_alias in SortCentre_alias:
            log(f"--------------------------------\n")
            log(f"Processing Sort Center: {SC_alias}\n")
            log(f"--------------------------------\n")

            # Fetch LMDC mappings once
            query = """
            SELECT
                l.id AS location_id, 
                l.alias AS location_alias, 
                cl.id AS next_location_id, 
                cl.alias AS next_location_alias,
                nm.crossdock_alias AS crossdock_alias
            FROM network_metadata nm
            JOIN locations l 
                ON nm.next_location_alias = l.alias 
            AND l.entity_type = 'PARTNER' 
            AND l.status = 1
            JOIN locations cl 
                ON cl.alias = %s 
            AND cl.entity_type = 'PARTNER' 
            AND cl.status = 1
            WHERE nm.location_alias = CONCAT(%s, '.LMSC')
            AND nm.is_active = 1
            AND l.location_ops_type = 'LM'
            AND (nm.crossdock_alias IS NULL OR nm.crossdock_alias = '')
            AND l.entity_id NOT IN ('127788', '127798', '127869', '128146')
            """
            with conn.cursor() as cursor:
                cursor.execute(query, (SC_alias, SC_alias))
                log10_query = cursor.fetchall()

            if not log10_query:
                log(f"No data found for {SC_alias}, skipping...")
                summary_data.append({"sc": SC_alias, "lmdc_count": 0, "inserted": 0})
                continue  

            df = pd.DataFrame(log10_query, columns=["location_id", "location_alias", "next_location_id", "next_location_alias", "sc_alias"])
            locations = list(df["location_id"])
            id_to_alias = dict(zip(df["location_id"], df["location_alias"]))

            log(f"LMDCs for {SC_alias}: {', '.join([f'{loc_id} ({alias})' for loc_id, alias in id_to_alias.items()])}")

            # Cache SC id for reuse
            with conn.cursor() as cursor:
                cursor.execute("SELECT id FROM locations WHERE entity_type = 'PARTNER' and alias = %s AND status = 1 AND location_ops_type = 'SC'", (SC_alias,))
                sc_rows = cursor.fetchall()
                if len(sc_rows) > 1:
                    log(f"Warning: Multiple active SC rows found for alias {SC_alias}: {[r['id'] for r in sc_rows]}")
                sc_id = sc_rows[0]['id'] if sc_rows else None

            # Collect inserts to batch later
            inserts = []

            for i in locations:
                for j in locations:
                    if i == j:
                        continue

                    from_alias = id_to_alias.get(i, 'Unknown')
                    to_alias = id_to_alias.get(j, 'Unknown')
                    log(f"Processing LMDC: {i} ({from_alias}) → {j} ({to_alias}) for SC: {SC_alias}")

                    with conn.cursor() as cursor:
                        cursor.execute("SELECT id, entity_id FROM locations WHERE id = %s", (i,))
                        from_loc_data = cursor.fetchone()
                        cursor.execute("SELECT id, entity_id, pincode_id FROM locations WHERE id = %s", (j,))
                        to_loc_data = cursor.fetchone()

                    if not from_loc_data or not to_loc_data:
                        log(f"Skipping {i}->{j}, missing data")
                        continue

                    with conn.cursor() as cursor:
                        cursor.execute("""
                        SELECT id FROM next_location_configs 
                        WHERE location_id = %s 
                          AND next_location_id = %s 
                          AND pincode_id = %s 
                          AND entity_type = 'MANIFEST' AND is_active = 1
                        """, (from_loc_data['id'], sc_id, to_loc_data['pincode_id']))
                        exists = cursor.fetchone()

                    if not exists:
                        inserts.append((
                            10823, from_loc_data['id'], sc_id, to_loc_data['pincode_id'],
                            1, 'MANIFEST', 1, 1, 'WF_BAGGING_CRON'
                        ))

            # Batch insert
            if inserts:
                with conn.cursor() as cursor:
                    cursor.executemany("""
                    INSERT INTO loadshare.next_location_configs 
                        (customer_id, location_id, next_location_id, pincode_id, return_available, 
                         entity_type, is_active, is_manual, audit_log) 
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """, inserts)
                conn.commit()
                inserted_count = len(inserts)
                log(f"Inserted {inserted_count} configs for SC: {SC_alias}")
            else:
                inserted_count = 0
                log(f"No new configs to insert for SC: {SC_alias}")

            # ✅ add to summary
            summary_data.append({
                "sc": SC_alias,
                "lmdc_count": len(locations),
                "inserted": inserted_count
            })

            log(f"Completed processing Sort Center: {SC_alias}\n")

        # ====== Print + Save Summary ======
        summary_table = []
        for entry in summary_data:
            summary_table.append([
                entry["sc"],
                entry["lmdc_count"],
                entry["inserted"]
            ])

        print("\n===== SUMMARY =====")
        print(tabulate(summary_table, headers=["SC", "No. of LMDCs", "Inserted Configs"], tablefmt="pretty"))

        with open("summary_output.csv", "w") as f:
            f.write("SC,No_of_LMDCs,Inserted_Configs\n")
            for row in summary_table:
                f.write(",".join(map(str, row)) + "\n")

        log("Script execution completed.")

except Exception as e:
    log(f"Error: {e}")

# finally:
#     if conn:
#         conn.close()
#         log("MySQL connection closed.")

finally:
    try:
        if conn and conn.open:
            conn.close()
            log("MySQL connection closed.")
    except NameError:
        log("MySQL connection was already closed.")

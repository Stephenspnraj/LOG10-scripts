import pandas as pd
import pymysql
import time
import logging
import sys
import math
from collections import defaultdict

# ========== CONFIG ==========
CSV_PATH = "cep_update_batch.csv"
LOG_FILE = "cep_update_output.log"
DRY_RUN = False
WAYBILL_BATCH_SIZE = 200  # chunk size for IN clause
# ============================

# Database connection details
# mysql_host = 'log10-staging.cco3osxqlq4g.ap-south-1.rds.amazonaws.com'
# mysql_port = 3306  # Assuming default MySQL port
# mysql_user = 'log10_staging'
# mysql_password = 'A_edjsHKmDF6vajhL4go6ekP'
# mysql_db = 'loadshare'

mysql_host = 'log10-replica-replica.cco3osxqlq4g.ap-south-1.rds.amazonaws.com'
mysql_port = 3306  
mysql_user = 'log10_scripts'
mysql_password = 'D2lx7Wz0Wm9ISAY-Vp1gxKDTzLRRl5k1m'
mysql_db = 'loadshare'

# ==== Setup Logging ====
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
console = logging.StreamHandler(sys.stdout)
console.setLevel(logging.INFO)
formatter = logging.Formatter("%(message)s")
console.setFormatter(formatter)
logging.getLogger().addHandler(console)

# ==== MySQL Connection ====
conn = pymysql.connect(
    host=mysql_host, 
    port=mysql_port,
    user=mysql_user, 
    password=mysql_password,
    db=mysql_db, 
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor,
    autocommit=not DRY_RUN
)

# mysql_host = 'log10-staging.cco3osxqlq4g.ap-south-1.rds.amazonaws.com'
# mysql_port = 3306  # Assuming default MySQL port
# mysql_user = 'log10_staging'
# mysql_password = 'A_edjsHKmDF6vajhL4go6ekP'
# mysql_db = 'loadshare'
# ==== Load & Clean CSV ====
df = pd.read_csv(CSV_PATH)
df.columns = [col.strip().lower() for col in df.columns]
df = df.dropna(subset=['waybill_no', 'old_location', 'new_location'])
df = df.drop_duplicates()

# ==== Grouping by (old_location, new_location) ====
grouped = defaultdict(list)
for _, row in df.iterrows():
    grouped[(row['old_location'], row['new_location'])].append(row['waybill_no'])

# ==== Fetch location_id map ====
all_aliases = set()
for old_location, new_location in grouped.keys():
    all_aliases.add(old_location)
    all_aliases.add(new_location)

def get_location_id_map(aliases):
    placeholders = ','.join(['%s'] * len(aliases))
    query = f"SELECT id, alias FROM locations WHERE alias IN ({placeholders})"
    with conn.cursor() as cursor:
        cursor.execute(query, list(aliases))
        results = cursor.fetchall()
    return {row['alias']: row['id'] for row in results}

alias_to_id = get_location_id_map(all_aliases)

# ==== Execute Updates ====
def chunk_list(lst, chunk_size):
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]

total_groups = len(grouped)
group_counter = 1
total_updates = 0

for (old_location, new_location), waybill_list in grouped.items():
    old_id = alias_to_id.get(old_location)
    new_id = alias_to_id.get(new_location)

    if not old_id or not new_id:
        logging.warning(f"⚠️  Skipping group {old_location} → {new_location}: missing location_id")
        continue

    for batch in chunk_list(waybill_list, WAYBILL_BATCH_SIZE):
        placeholders = ','.join(['%s'] * len(batch))
        update_query = f"""
            UPDATE consignment_expected_path
            SET location_id = %s, updated_at = NOW()
            WHERE location_id = %s
              AND waybill_no IN ({placeholders})
              AND is_client_path = 1
        """
        params = [new_id, old_id] + batch

        if DRY_RUN:
            logging.info(f"🔍 DRY-RUN: Would update {len(batch)} waybills for {old_location} → {new_location}")
            # logging.info(update_query, params)
            file_handler = logging.getLogger().handlers[0]
            file_handler.emit(logging.makeLogRecord({"msg": update_query, "levelno": logging.INFO, "levelname": "INFO"}))
            file_handler.emit(logging.makeLogRecord({"msg": str(params), "levelno": logging.INFO, "levelname": "INFO"}))
        else:
            with conn.cursor() as cursor:
                # logging.info(update_query)
                # logging.info(params)
                logging.info(f"Would update {len(batch)} waybills for {old_location} → {new_location}")
                file_handler = logging.getLogger().handlers[0]
                file_handler.emit(logging.makeLogRecord({"msg": update_query, "levelno": logging.INFO, "levelname": "INFO"}))
                file_handler.emit(logging.makeLogRecord({"msg": str(params), "levelno": logging.INFO, "levelname": "INFO"}))
                cursor.execute(update_query, params)
                #logging.info(f"✅ Updated {cursor.rowcount} rows for {old_location} → {new_location}")
                logging.info(f"✅ Updated {cursor.rowcount} rows for {old_location} → {new_location}")
                total_updates += cursor.rowcount

    logging.info(f"✅ Finished group {group_counter}/{total_groups}: {old_location} → {new_location}")
    group_counter += 1
    time.sleep(0.3)  # mild pause between groups

conn.close()
logging.info(f"🎉 Script complete. Total updates: {total_updates}")

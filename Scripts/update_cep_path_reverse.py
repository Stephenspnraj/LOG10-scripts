import os
import time
import pymysql
import pandas as pd
from datetime import datetime, timedelta
from contextlib import contextmanager

# ------------------------------
# CONFIG
# ------------------------------
LOCATION_BATCH_SIZE = 500  # number of location IDs per batch
OUTPUT_CSV_PATH = "updated_waybills_output.csv"

CHOICE = os.getenv('CHOICE', '1')  # 1 = last 24h, 2 = custom date
DATE_PARAM = os.getenv('DATE')     # required if CHOICE=2

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "log10-replica-replica.cco3osxqlq4g.ap-south-1.rds.amazonaws.com"),
    "user": os.getenv("DB_USER", "log10_scripts"),
    "password": os.getenv("DB_PASSWORD", "D2lx7Wz0Wm9ISAY-Vp1gxKDTzLRRl5k1m"),
    "database": os.getenv("DB_NAME", "loadshare"),
    "port": int(os.getenv("DB_PORT", 3306)),
}

# ------------------------------
# DB CONNECTION
# ------------------------------
@contextmanager
def get_connection():
    conn = pymysql.connect(
        host=DB_CONFIG["host"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        db=DB_CONFIG["database"],
        port=DB_CONFIG["port"],
        cursorclass=pymysql.cursors.DictCursor,
        charset="utf8mb4",
        autocommit=False
    )
    try:
        yield conn
    finally:
        conn.close()

# ------------------------------
# DATE RANGE
# ------------------------------
def get_date_range():
    if CHOICE == "1":
        end_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        start_date = (datetime.now() - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")
    elif CHOICE == "2":
        if not DATE_PARAM:
            raise ValueError("DATE environment variable not set for custom date")
        start_date = f"{DATE_PARAM} 00:00:01"
        end_date = f"{DATE_PARAM} 23:59:59"
    else:
        print("Invalid choice! Defaulting to Last 24 Hours.")
        end_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        start_date = (datetime.now() - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")
    print(f"📅 Running for: {start_date} → {end_date}")
    return start_date, end_date

# ------------------------------
# FETCH LOCATION IDs
# ------------------------------
def fetch_location_ids(conn):
    query = """
        SELECT id
        FROM locations
        WHERE location_ops_type = 'LM'
          AND entity_type = 'PARTNER'
          AND status = 1
          AND alias LIKE %s
    """
    with conn.cursor() as cursor:
        cursor.execute(query, ('%/%',))
        result = cursor.fetchall()
    location_ids = [row['id'] for row in result]
    print(f"✅ Fetched {len(location_ids)} location IDs")
    return location_ids

# ------------------------------
# FETCH WAYBILLS BY LOCATION BATCH
# ------------------------------
def fetch_waybills_by_batches(conn, start_date, end_date, location_ids, batch_size=LOCATION_BATCH_SIZE):
    if not location_ids:
        print("⚠️ No location IDs found. Exiting.")
        return []

    all_waybills = []

    for i in range(0, len(location_ids), batch_size):
        batch = location_ids[i:i + batch_size]
        placeholders = ','.join(['%s'] * len(batch))
        query = f"""
            SELECT cep.waybill_no
            FROM consignment_expected_path cep
            WHERE cep.created_at BETWEEN %s AND %s
              AND cep.is_client_path = 1
              AND cep.flow_type = 'RTO'
              AND cep.`index` IN (2,3,4,5)
              AND cep.location_id IN ({placeholders})
        """
        params = [start_date, end_date] + batch
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            result = cursor.fetchall()
        waybills = [row['waybill_no'] for row in result]
        all_waybills.extend(waybills)
        print(f"🧾 Batch {i // batch_size + 1}: {len(waybills)} waybills fetched")

    print(f"✅ Total waybills fetched: {len(all_waybills)}")
    return all_waybills

# ------------------------------
# UPDATE WAYBILLS
# ------------------------------
def update_waybills(conn, waybill_numbers, batch_size=500):
    if not waybill_numbers:
        print("⚠️ No waybills to update. Exiting.")
        return

    for i in range(0, len(waybill_numbers), batch_size):
        batch = waybill_numbers[i:i + batch_size]
        placeholders = ','.join(['%s'] * len(batch))

        update_query = f"""
            UPDATE consignment_expected_path cep
            JOIN (
                SELECT 
                    waybill_no,
                    location_id,
                    `index`,
                    location_type,
                    ROW_NUMBER() OVER (PARTITION BY waybill_no ORDER BY `index` DESC) - 1 AS new_index
                FROM consignment_expected_path
                WHERE waybill_no IN ({placeholders})
                  AND is_client_path = 1
                  AND flow_type = 'RTO'
            ) t
              ON cep.waybill_no = t.waybill_no
             AND cep.location_id = t.location_id
             AND cep.`index` = t.`index`
            SET cep.`index` = t.new_index
            WHERE cep.waybill_no IN ({placeholders})
              AND cep.is_client_path = 1
              AND cep.flow_type = 'RTO';
        """
        params = batch + batch
        with conn.cursor() as cursor:
            cursor.execute(update_query, params)
        conn.commit()
        print(f"⚡ Batch {i // batch_size + 1}: {len(batch)} waybills updated")
        time.sleep(1)

# ------------------------------
# MAIN
# ------------------------------
if __name__ == "__main__":
    start_date, end_date = get_date_range()
    with get_connection() as conn:
        location_ids = fetch_location_ids(conn)
        waybills = fetch_waybills_by_batches(conn, start_date, end_date, location_ids)

        if not waybills:
            print("⚠️ No waybills found for update. Exiting.")
        else:
            print(f"🔧 Starting update for {len(waybills)} waybills...")
            update_waybills(conn, waybills)
            
            # save output
            df = pd.DataFrame({"waybill_no": waybills})
            df.to_csv(OUTPUT_CSV_PATH, index=False)
            print(f"📁 Output saved to: {OUTPUT_CSV_PATH}")
            print("✅ Script completed successfully.")

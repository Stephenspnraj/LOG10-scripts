import pymysql
import pandas as pd
from time import sleep

# ===== Database connection details =====
mysql_host = 'log10-replica-replica.cco3osxqlq4g.ap-south-1.rds.amazonaws.com'
mysql_port = 3306
mysql_user = 'log10_scripts'
mysql_password = 'D2lx7Wz0Wm9ISAY-Vp1gxKDTzLRRl5k1m'
mysql_db = 'loadshare'

# ===== File paths =====
batch_log_file = 'batch_log.txt'
csv_file = 'deactivate_nlc.csv'  # Input CSV with "id" column

# ===== Config =====
batch_size = 200
audit_log_text = "Duplicate manifest records cleanup activity"

# ===== Read IDs =====
ids_df = pd.read_csv(csv_file)
ids_list = ids_df['id'].astype(str).tolist()  # Ensures all IDs are strings

def update_records(conn, batch, batch_number):
    """Update next_location_configs in batches."""
    try:
        with conn.cursor() as cursor:
            query = f"""
                UPDATE next_location_configs
                SET audit_log = %s,
                is_active = 0
                WHERE id IN ({','.join(['%s'] * len(batch))})
                  AND is_active = 1
            """
            params = [audit_log_text] + batch
            cursor.execute(query, params)
            conn.commit()
            print(f"[Batch {batch_number}] Updated {cursor.rowcount} records successfully.")

        with open(batch_log_file, 'a') as log_file:
            log_file.write(f"Batch {batch_number}: {','.join(batch)}\n")

    except Exception as e:
        print(f"[Batch {batch_number}] Error: {e}")

# ===== Main processing =====
if not ids_list:
    print("No IDs found in CSV. Exiting.")
else:
    try:
        conn = pymysql.connect(
            host=mysql_host,
            port=mysql_port,
            user=mysql_user,
            password=mysql_password,
            db=mysql_db,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )

        for i in range(0, len(ids_list), batch_size):
            batch = ids_list[i:i + batch_size]
            batch_number = (i // batch_size) + 1
            print(f"Processing batch {batch_number} ({len(batch)} records)...")
            update_records(conn, batch, batch_number)
            sleep(3)  # Reduce DB load

    finally:
        conn.close()
        print("Batch update process completed.")

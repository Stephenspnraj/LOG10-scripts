import pymysql
import pandas as pd

# Titan DB connection details
titan_config = {
    'host': 'prod-titan-rds.cco3osxqlq4g.ap-south-1.rds.amazonaws.com',
    'port': 3306,
    'user': 'hermes_scripts',
    'password': '7Xc8redscriptsicsEagQ',
    'db': 'titan',
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}

# Log10 DB connection details (similar to titan)
log10_config = {
    'host': 'log10-replica-replica.cco3osxqlq4g.ap-south-1.rds.amazonaws.com',
    'port': 3306,
    'user': 'log10_scripts',
    'password': 'D2lx7Wz0Wm9ISAY-Vp1gxKDTzLRRl5k1m',
    'db': 'loadshare',
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}

# Read input CSV
df = pd.read_csv('user_migration.csv')

# Connect to Titan and Log10
titan_conn = pymysql.connect(**titan_config)
log10_conn = pymysql.connect(**log10_config)

def migrate_and_update(contact_number, location_id):
    try:
        # Step 1: Call stored procedure in Titan
        with titan_conn.cursor() as cursor:
            cursor.execute(f"""
                SELECT
                    CONCAT('call update_users_ecom(', u.id, ',', h.id, ',', h.organization_id, ');') AS call_statement
                FROM users u
                JOIN hubs h ON h.zone_id IN (
                    SELECT id FROM zones WHERE order_broker = %s
                )
                WHERE u.contact_number = %s
                  AND u.role = 3
                  AND u.is_active = 1
                  AND h.is_active = 1
                  AND h.primary_operation = 'ECOM'
            """, (location_id, contact_number))
            result = cursor.fetchone()
            if not result:
                print(f"[SKIP] No match for contact_number={contact_number}, location_id={location_id}")
                return

            call_stmt = result['call_statement']
            print(f"[INFO] Executing in Titan: {call_stmt}")
            cursor.execute(call_stmt)
            titan_conn.commit()

        # Step 2: Get new user_id in Titan (as staffpay_user_id)
        with titan_conn.cursor() as cursor:
            cursor.execute("""
                SELECT id FROM users
                WHERE contact_number = %s
                  AND role = 3
                  AND is_active = 1
                ORDER BY id DESC LIMIT 1
            """, (contact_number,))
            titan_user = cursor.fetchone()
            if not titan_user:
                print(f"[ERROR] Failed to retrieve new user_id from Titan for {contact_number}")
                return
            new_user_id = titan_user['id']

        # Step 3: Get corresponding user_id in Log10
        with log10_conn.cursor() as cursor:
            cursor.execute("""
                SELECT id FROM users
                WHERE contact_number = %s AND status = 1
            """, (contact_number,))
            log10_user = cursor.fetchone()
            if not log10_user:
                print(f"[ERROR] No matching user_id in Log10 for contact_number={contact_number}")
                return
            log10_user_id = log10_user['id']

        # Step 4: Update user_subsystem_mapping in Log10
        # Step 4: Check if user_subsystem_mapping exists and update in Log10
        with log10_conn.cursor() as cursor:
            # First, check if a mapping record exists
            cursor.execute("""
                SELECT 1 FROM user_subsystem_mapping
                WHERE user_id = %s
            """, (log10_user_id,))
            mapping_exists = cursor.fetchone()

            if not mapping_exists:
                print(f"[VALIDATION ERROR] No user_subsystem_mapping found for user_id={log10_user_id} in Log10. Skipping update.")
                return

            # If exists, proceed to update
            cursor.execute("""
                UPDATE user_subsystem_mapping
                SET staffpay_user_id = %s
                WHERE user_id = %s
            """, (new_user_id, log10_user_id))
            log10_conn.commit()
            print(f"[SUCCESS] Updated staffpay_user_id in Log10 for user_id={log10_user_id} to {new_user_id}")


    except Exception as e:
        print(f"[EXCEPTION] {e} while processing contact_number={contact_number}")

# Loop over all input rows
# for _, row in df.iterrows():
#     migrate_and_update(row['contact_number'], row['location_id'])
for idx, row in df.iterrows():
    contact_number = row['contact_number']
    location_id = row['location_id']
    print(f"\n--- Processing Row {idx + 1}: contact_number={contact_number}, location_id={location_id} ---")
    migrate_and_update(contact_number, location_id)


# Close DB connections
titan_conn.close()
log10_conn.close()

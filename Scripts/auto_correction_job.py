import pymysql
from prettytable import PrettyTable
from time import sleep

# MySQL connection config
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

conn = pymysql.connect(
    host=mysql_host,
    port=mysql_port,
    user=mysql_user,
    password=mysql_password,
    db=mysql_db,
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor
)

def validate_json():
    with conn.cursor() as cursor:
        cursor.execute("SELECT JSON_VALID(config) as is_valid FROM application_config WHERE id = 1")
        res = cursor.fetchone()
        print("\n✅ JSON Validation Result:", "Valid" if res['is_valid'] == 1 else "Invalid")

def print_table(data, headers):
    table = PrettyTable()
    table.field_names = headers
    for row in data:
        table.add_row([row[col] for col in headers])
    print(table)

def update_bypass_inscan():
    print("\n==== Bypass Inscan Location Update ===")
    validate_json()
    select_query = """
        SELECT lfm.id as location_id,
        lfm.alias as location_name,
p.name as partnet_name,
lfm.created_at as created_at
        FROM locations lfm 
        LEFT JOIN application_config ac ON ac.id = 1
        LEFT JOIN JSON_TABLE(
            ac.config,
            '$.bypassInscanLocationIds[*]' 
            COLUMNS (
                loc INT PATH '$'
            )
        ) jt ON lfm.id = jt.loc
        JOIN partners p on p.id=lfm.entity_id
        WHERE lfm.entity_type = 'partner' 
          AND lfm.location_ops_type='FM'
          AND lfm.status = 1
          AND lfm.is_valmo_location=1
          AND jt.loc IS NULL
          AND lfm.entity_id NOT IN (127788,127869,129055)
        GROUP BY lfm.id;
    """
    with conn.cursor() as cursor:
        cursor.execute(select_query)
        rows = cursor.fetchall()

        if rows:
            print_table(rows, ["location_id","location_name" ,"created_at" ])
            for row in rows:
                update_query = """
                    UPDATE application_config
                    SET config = JSON_SET(
                        config,
                        '$.bypassInscanLocationIds',
                        JSON_ARRAY_APPEND(
                            JSON_EXTRACT(config, '$.bypassInscanLocationIds'),
                            '$',
                            %s
                        )
                    )
                    WHERE id = 1
                """
                cursor.execute(update_query, (row["location_id"],))
            conn.commit()
            print(f"✅ Updated {len(rows)} location(s) in bypassInscanLocationIds")
            #print(f"✅ Updated {cursor.rowcount} location(s) in bypassInscanLocationIds")
        else:
            print("ℹ️ No locations to update for bypassInscanLocationIds")

def update_auto_pickup_location():
    print("\n=== Auto Pickup Location Config Update ===")
    validate_json()
    select_query = """
        SELECT plc.pickup_location_id,
               l.alias as location_name,
               l.updated_at
        FROM pickup_location_configs plc
        LEFT JOIN application_config ac ON ac.id = 1
        LEFT JOIN JSON_TABLE(
            ac.config,
            '$.customer_10823.auto_pickup_location[*]' 
            COLUMNS (
                loc INT PATH '$'
            )
        ) jt ON plc.pickup_location_id = jt.loc
        JOIN locations l on l.id=plc.pickup_location_id AND l.entity_type='customer'
        WHERE plc.customer_id = 10823 
          AND plc.is_active = 1
          AND jt.loc IS NULL
          AND plc.pickup_location_id NOT IN ('5880820','5880955','5880957','5880966','5881394','5881396','5881398')
        GROUP BY plc.pickup_location_id;
    """
    with conn.cursor() as cursor:
        cursor.execute(select_query)
        rows = cursor.fetchall()

        if rows:
            print_table(rows, ["pickup_location_id", "location_name", "updated_at"])
            for row in rows:
                update_query = """
                    UPDATE application_config
                    SET config = JSON_SET(
                        config,
                        '$.customer_10823.auto_pickup_location',
                        JSON_ARRAY_APPEND(
                            JSON_EXTRACT(config, '$.customer_10823.auto_pickup_location'),
                            '$',
                            %s
                        )
                    )
                    WHERE id = 1
                """
                cursor.execute(update_query, (row["pickup_location_id"],))
            conn.commit()
            print(f"✅ Updated {len(rows)} pickup location(s) in auto_pickup_location")
            #print(f"✅ Updated {cursor.rowcount} pickup location(s) in auto_pickup_location")
        else:
            print("ℹ️ No pickup locations to update")

# def disable_wrong_facility():
#     print("\n=== Disable Wrong Facility Config ===")
#     select_query = """
#         SELECT
#           nlc.id as nlc_id,
#           l.alias AS location,
#           ln.alias AS next_location_name,
#           ld.alias AS dest_location_name,
#           nlc.updated_at
#         FROM next_location_configs nlc
#         JOIN locations l ON nlc.location_id = l.id 
#         JOIN locations ln ON nlc.next_location_id = ln.id
#         JOIN locations ld ON ld.pincode_id = nlc.pincode_id 
#           AND ld.entity_type = 'partner'
#           AND ld.is_valmo_location = 1 
#           AND ld.location_ops_type = 'LM'
#           AND ld.status = 1
#           AND ln.alias <> 'GHS'
#           AND ln.alias NOT LIKE '%BLS%'
#           AND ld.client_location_name NOT LIKE '%OLD'
#           AND l.client_location_name NOT LIKE '%OLD'
#         WHERE 
#           nlc.is_manual = 1 
#           AND nlc.is_active = 1 
#           AND l.location_ops_type = 'LM'  
#           AND l.is_valmo_location = 1 
#           AND nlc.entity_type = 'manifest'
#           AND (
#               (ln.alias <> SUBSTRING(l.alias, 4, 3))
#               OR 
#               (ln.alias <> SUBSTRING(ld.alias, 4, 3))
#           )
#         ORDER BY nlc.id DESC;
#     """
#     with conn.cursor() as cursor:
#         cursor.execute(select_query)
#         rows = cursor.fetchall()

#         if rows:
#             print(f"Total records to be disabled: {len(rows)}\n")
#             print_table(rows[:20], ["nlc_id", "location", "next_location_name", "dest_location_name", "updated_at"])
#             batch_size = 300
#             for i in range(0, len(rows), batch_size):
#                 batch_ids = [str(row["nlc_id"]) for row in rows[i:i + batch_size]]
#                 update_query = f"""
#                     UPDATE next_location_configs 
#                     SET is_active=0, audit_log='old wrongFacility deactivate'
#                     WHERE id IN ({','.join(batch_ids)})
#                 """
#                 cursor.execute(update_query)
#                 print(f"✅ Updated batch {i//batch_size + 1}: {len(batch_ids)} records\n")
#             conn.commit()
#         else:
#             print("ℹ️ No wrong facility records to disable")
def disable_wrong_facility():
    print("\n=== Disable Wrong Facility Config ===")
    select_query = """
        SELECT
          nlc.id as nlc_id,
          l.alias AS location,
          ln.alias AS next_location_name,
          ld.alias AS dest_location_name,
          nlc.updated_at
        FROM next_location_configs nlc
        JOIN locations l ON nlc.location_id = l.id 
        JOIN locations ln ON nlc.next_location_id = ln.id
        JOIN locations ld ON ld.pincode_id = nlc.pincode_id 
          AND ld.entity_type = 'partner'
          AND ld.is_valmo_location = 1 
          AND ld.location_ops_type = 'LM'
          AND ld.status = 1
          AND ln.alias <> 'GHS'
          and ln.alias <> 'S2/BLS/6/VDQ'
          AND ld.client_location_name NOT LIKE '%OLD'
          AND l.client_location_name NOT LIKE '%OLD'
        WHERE 
        
          nlc.is_manual = 1 
          AND nlc.is_active = 1 
        and (nlc.audit_log IS NULL OR nlc.audit_log <> 'multi-sc wrong facility')
          AND l.location_ops_type = 'LM'  
          AND l.is_valmo_location = 1 
          AND nlc.entity_type = 'manifest'
          AND (
              (ln.alias <> SUBSTRING(l.alias, 4, 3))
              OR 
              (ln.alias <> SUBSTRING(ld.alias, 4, 3))
          )
        ORDER BY nlc.id DESC;
    """

    batch_log_file = "wrong_facility_batch_log.txt"
    batch_size = 300

    try:
        with conn.cursor() as cursor:
            cursor.execute(select_query)
            rows = cursor.fetchall()

        if not rows:
            print("ℹ️ No wrong facility records to disable")
            return

        print(f"Total records to be disabled: {len(rows)}\n")
        print_table(rows[:10], ["nlc_id", "location", "next_location_name", "dest_location_name", "updated_at"])

        ids_list = [str(row["nlc_id"]) for row in rows]

        for i in range(0, len(ids_list), batch_size):
            batch = ids_list[i:i + batch_size]
            batch_number = i // batch_size + 1
            print(f"Processing batch {batch_number} ({len(batch)} records)...")

            try:
                with conn.cursor() as cursor:
                    query = f"""UPDATE next_location_configs 
                                SET audit_log='old wrongFacility deactivate', is_active = %s 
                                WHERE id IN ({','.join(['%s'] * len(batch))})"""
                    cursor.execute(query, [0] + batch)
                    conn.commit()
                    print(f"✅ Updated {cursor.rowcount} records successfully.")

                with open(batch_log_file, 'a') as log_file:
                    log_file.write(f"Batch {batch_number}: {','.join(batch)}\n")

            except Exception as e:
                print(f"❌ Error updating batch {batch_number}: {e}")

            sleep(3)  # Throttle to reduce DB load

    except Exception as e:
        print(f"❌ Unexpected error: {e}")

# Run all functions
update_bypass_inscan()
update_auto_pickup_location()
#disable_wrong_facility()

# Close connection
conn.close()

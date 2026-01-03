import pymysql
import pandas as pd
import sys
from time import sleep

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

# Input parameters
FM_LOC_NAME = sys.argv[1]  
OLD_FMSC_LOC_NAME = sys.argv[2]  
NEW_SC = sys.argv[3]  
MODE = sys.argv[4].upper()  

# If UPDATE mode is selected, batch_size must be provided
BATCH_SIZE = int(sys.argv[5]) if MODE == "UPDATE" and len(sys.argv) > 5 else 200

# Output files
BATCH_LOG_FILE = 'batch_log.txt'
OUTPUT_FILE = 'output.csv'

# Validate required parameters
if not FM_LOC_NAME or not OLD_FMSC_LOC_NAME or not NEW_SC:
    print("Error: FM, OLD_SC, and NEW_SC must be provided.")
    sys.exit(1)

# Database connection function
def get_db_connection():
    return pymysql.connect(
        host=mysql_host,
        port=mysql_port,
        user=mysql_user,
        password=mysql_password,
        db=mysql_db,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

# Validate locations exist in the database
try:
    conn = get_db_connection()
    with conn.cursor() as cursor:
        query = """
            SELECT alias FROM locations WHERE alias IN (%s, %s, %s);
        """
        cursor.execute(query, (FM_LOC_NAME, OLD_FMSC_LOC_NAME, NEW_SC))
        existing_locations = {row['alias'] for row in cursor.fetchall()}

    # Check if all three locations exist
    missing_locations = {FM_LOC_NAME, OLD_FMSC_LOC_NAME, NEW_SC} - existing_locations
    if missing_locations:
        print(f"Error: The following locations are missing in the database: {', '.join(missing_locations)}")
        sys.exit(1)

    print(f"All locations exist in the database: {FM_LOC_NAME}, {OLD_FMSC_LOC_NAME}, {NEW_SC}")

    # Fetch location_id for NEW_SC
    with conn.cursor() as cursor:
        cursor.execute("SELECT id FROM locations WHERE alias = %s", (NEW_SC,))
        location_row = cursor.fetchone()
        if not location_row:
            print(f"Error: Location ID not found for alias {NEW_SC}")
            sys.exit(1)
        LOCATION_ID = str(location_row['id'])

except Exception as e:
    print(f"Error checking location aliases: {e}")
    sys.exit(1)
finally:
    conn.close()

# Fetch IDs and full data based on SQL query
def get_records_to_update():
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            query = """
                SELECT cep.id, cep.waybill_no, cep.index, cep.location_id, l.alias, 
                       cep.is_client_path, cep.location_type, cep.created_at, cep.updated_at, cep.flow_type
                FROM consignment_expected_path cep
                JOIN consignments c 
                    ON c.waybill_no = cep.waybill_no
                    AND c.partner_id = 268
                    AND c.flow_type = 'FORWARD'
                JOIN locations ld ON ld.id = cep.location_id    
                JOIN locations l ON l.id = c.location_id
                WHERE cep.waybill_no IN (
                    SELECT DISTINCT cep.waybill_no
                    FROM consignment_expected_path cep 
                    JOIN consignments c 
                        ON c.waybill_no = cep.waybill_no
                    JOIN locations l ON l.id = c.location_id    
                        AND c.partner_id = 268
                        AND cep.is_client_path = 1 
                        AND c.consignment_status NOT IN ('SL', 'BOOKING_CANCELLED', 'DEL', 'RTODEL', 'IN_TRANSIT', 'MANI_LINK')
                        AND cep.index = 0 
                        AND c.location_id = cep.location_id
                        AND l.alias = %s
                        AND c.created_at > CURRENT_DATE - INTERVAL 4 MONTH
                )
                AND cep.index = 1
                AND ld.alias = %s
                AND cep.flow_type = 'FORWARD'
                AND cep.location_type <> 'CD'
                AND cep.is_client_path = 1;
            """
            cursor.execute(query, (FM_LOC_NAME, OLD_FMSC_LOC_NAME))
            result = cursor.fetchall()
            return result  # Returning full data for DRYRUN

    except Exception as e:
        print(f"Error fetching records: {e}")
        sys.exit(1)
    finally:
        conn.close()

# Fetch records
records_list = get_records_to_update()
ids_list = [str(row['id']) for row in records_list]  # Extracting only IDs for update

# DRYRUN Mode: Write full data to CSV
if MODE == "DRYRUN":
    print(f"Total records found: {len(records_list)}")

    if records_list:
        # Extract column names dynamically
        column_names = records_list[0].keys()
        with open(OUTPUT_FILE, 'w') as file:
            file.write(",".join(column_names) + "\n")
            for row in records_list:
                file.write(",".join(str(row[col]) for col in column_names) + "\n")

        print(f"Output written to {OUTPUT_FILE}")
    else:
        print("No records found, output file not created.")
    
    sys.exit(0)

# UPDATE Mode: Process and update records in batches
elif MODE == "UPDATE":
    if BATCH_SIZE <= 0:
        print("Error: Batch size must be greater than 0 for UPDATE mode.")
        sys.exit(1)
    if records_list:
        # Extract column names dynamically
        column_names = records_list[0].keys()
        with open(OUTPUT_FILE, 'w') as file:
            file.write(",".join(column_names) + "\n")
            for row in records_list:
                file.write(",".join(str(row[col]) for col in column_names) + "\n")

        print(f"Output written to {OUTPUT_FILE}")
    else:
        print("No records found, output file not created.")
    
  

    #print(f"Running in UPDATE mode with batch size: {BATCH_SIZE}",f"Total records found: {len(records_list)}")
    #print(f"Total records found: {len(records_list)}\n",f"Running in UPDATE mode with batch size: {BATCH_SIZE}")
    print(f"Total records found: {len(records_list)}")
    print(f"Running in UPDATE mode with batch size: {BATCH_SIZE}")
    # Function to update records in batches
    def update_records(batch, batch_number):
        try:
            conn = get_db_connection()
            with conn.cursor() as cursor:
                query = f"""UPDATE consignment_expected_path SET location_id = %s 
                            WHERE id IN ({','.join(['%s'] * len(batch))})"""
                cursor.execute(query, [LOCATION_ID] + batch)
                conn.commit()
                print(f"Updated {cursor.rowcount} records in batch {batch_number}.")
            with open(BATCH_LOG_FILE, 'a') as log_file:
                log_file.write(f"Batch {batch_number}: {','.join(batch)}\n")
        except Exception as e:
            print(f"Error updating batch: {e}")
        finally:
            conn.close()

    # Process records in batches
    for i in range(0, len(ids_list), BATCH_SIZE):
        batch = ids_list[i:i + BATCH_SIZE]
        batch_number = i // BATCH_SIZE + 1
        print(f"Processing batch {batch_number} ({len(batch)} records)...")
        update_records(batch, batch_number)
        sleep(5)  # Sleep to reduce database load

    print("Batch update process completed.")

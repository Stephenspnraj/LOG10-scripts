import csv
import pymysql

# Stagging
mysql_host = 'log10-replica-replica.cco3osxqlq4g.ap-south-1.rds.amazonaws.com'
mysql_port = 3306  # Assuming default MySQL port
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

# Function to execute select query and return result
def execute_select_query(cursor, manifest_code, alias):
    select_query = f"""
    SELECT
        m.id as source_entity_id,
        cc.id as destination_entity_id
    FROM
        manifests m
    JOIN
        locations ld on ld.id = m.destination_loc_id
    JOIN
        entity_consignment_mapping ecm on ecm.source_entity_id = m.id
    JOIN
        connections cc on cc.id = ecm.destination_entity_id
    JOIN
        locations scl on cc.originated_loc_id = scl.id
    WHERE
        m.manifest_code = '{manifest_code}' and scl.alias = '{alias}'
    GROUP BY
        m.id, cc.id
    """
    cursor.execute(select_query)
    return cursor.fetchone()

# Function to execute delete query
def execute_delete_query(cursor, source_entity_id, destination_entity_id):
    count_query = f"SELECT COUNT(*) FROM entity_consignment_mapping WHERE source_entity_id = {source_entity_id} AND destination_entity_id = {destination_entity_id}"
    cursor.execute(count_query)
    count_result = cursor.fetchone()
    
    if count_result['COUNT(*)'] == 1:
        delete_query = f"DELETE FROM entity_consignment_mapping WHERE source_entity_id = {source_entity_id} AND destination_entity_id = {destination_entity_id}"
        cursor.execute(delete_query)
        conn.commit()
        print(f"Deleted record with source_entity_id={source_entity_id} and destination_entity_id={destination_entity_id}")
        return True
    else:
        print(f"Skipped deletion for source_entity_id={source_entity_id} and destination_entity_id={destination_entity_id} due to multiple records")
        return False

# Function to execute update query
def execute_update_query(cursor, manifest_code, alias):
    select_location_query = f"SELECT id FROM locations WHERE alias='{alias}'"
    cursor.execute(select_location_query)
    location_id = cursor.fetchone()
    
    if location_id:
        update_query = f"UPDATE manifests SET manifest_status='PENDING', current_loc_id='{location_id['id']}' WHERE manifest_code='{manifest_code}'"
        cursor.execute(update_query)
        conn.commit()
        print(f"Updated manifest_code={manifest_code} with location_id={location_id['id']}")
    else:
        print(f"No location found for alias={alias}")

# Read data from CSV and process each row
csv_file_path = 'input.csv'
with open(csv_file_path, mode='r') as file:
    csv_reader = csv.reader(file)
    total_rows = sum(1 for row in csv_reader) - 1  # Calculate total rows excluding header
    file.seek(0)  # Reset file pointer to beginning
    next(csv_reader)  # Skip header row
    
    with conn.cursor() as cursor:
        for i, row in enumerate(csv_reader, start=1):
            manifest_code = row[0]
            alias = row[1]
            
            print(f"Iteration: {i}/{total_rows}")
            print(f"Manifest_code: {manifest_code}")
            print("--------")
            
            query_result = execute_select_query(cursor, manifest_code, alias)
            
            if query_result:
                source_entity_id = query_result['source_entity_id']
                destination_entity_id = query_result['destination_entity_id']
                if execute_delete_query(cursor, source_entity_id, destination_entity_id):
                    execute_update_query(cursor, manifest_code, alias)
            else:
                print(f"No connections found for manifest_code={manifest_code} and alias={alias}. Skipping.")
            print()

# Close MySQL connection
conn.close()

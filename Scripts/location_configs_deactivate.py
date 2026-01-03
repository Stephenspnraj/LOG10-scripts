import pymysql
import csv

# Database configuration
mysql_host = 'log10-replica-replica.cco3osxqlq4g.ap-south-1.rds.amazonaws.com'
mysql_port = 3306
mysql_user = 'log10_scripts'
mysql_password = 'D2lx7Wz0Wm9ISAY-Vp1gxKDTzLRRl5k1m'
mysql_db = 'loadshare'


def db_connection():
    return pymysql.connect(
        host=mysql_host,
        port=mysql_port,
        user=mysql_user,
        password=mysql_password,
        db=mysql_db,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

# Step 1: Update NLC to manual
def update_nlc_to_manual(cursor, location_id):
    cursor.execute(f"SELECT id FROM next_location_configs WHERE (location_id={location_id} OR next_location_id={location_id}) AND is_active=1 AND is_manual=0")
    nlc_records = cursor.fetchall()

    if nlc_records:
        cursor.execute(f"UPDATE next_location_configs SET is_manual=1, audit_log='DEACTIVATE_CRON' WHERE (location_id={location_id} OR next_location_id={location_id}) AND is_active=1 AND is_manual=0")
        print(f"Step 1: Regular(SC-DC) NLC entries updated to manual for location ID {location_id}.")
    else:
        print(f"Step 1: No Regular(SC-DC) NLC entries to update to manual for location ID {location_id}.")

# Step 2: Deactivate WF config NLC
def deactivate_wf_config(cursor, pincode_id):
    cursor.execute(f"SELECT id FROM next_location_configs WHERE pincode_id={pincode_id} AND entity_type='MANIFEST' AND is_active=1 AND is_manual=1")
    wf_config_records = cursor.fetchall()

    if wf_config_records:
        cursor.execute(f"UPDATE next_location_configs SET is_active=0 WHERE pincode_id={pincode_id} AND entity_type='MANIFEST' AND audit_log='WF_BAGGING_CRON' AND is_active=1 AND is_manual=1")
        print(f"Step 2: WF config NLC entries deactivated for pincode ID {pincode_id}.")
    else:
        print(f"Step 2: No WF config NLC entries to deactivate for pincode ID {pincode_id}.")

# Step 3: Deactivate network metadata
def deactivate_network_metadata(cursor, alias):
    cursor.execute(f"SELECT is_active FROM network_metadata WHERE next_location_alias='{alias}' AND is_active=1")
    network_metadata_records = cursor.fetchall()

    if network_metadata_records:
        cursor.execute(f"UPDATE network_metadata SET is_active=0 WHERE next_location_alias='{alias}' AND is_active=1")
        print(f"Step 3: Network metadata de-activated for {alias}.")
    else:
        print(f"Step 3: No network metadata entries to deactivate for {alias}.")

# Step 4: Update client location name
def update_client_location_name(cursor, alias):
    cursor.execute(f"SELECT client_location_name FROM locations WHERE alias='{alias}' AND status=1")
    location_record = cursor.fetchone()

    if location_record:
        new_name = f"{location_record['client_location_name']}_old"
        cursor.execute(f"UPDATE locations SET client_location_name='{new_name}' WHERE alias='{alias}' AND status=1")
        print(f"Step 4: Client location name updated to '{new_name}' for {alias} and client_name: {location_record['client_location_name']}")
    else:
        print(f"Step 4: No client location name found for {alias} to update.")

# Step 5: Update serviceable_area_mapping
def update_serviceable_area_mapping(cursor, location_id):
    cursor.execute(f"SELECT * FROM serviceable_area_mapping WHERE location_id={location_id} AND is_active=1 AND audit_log<>'MIGRATED'")
    serviceable_records = cursor.fetchall()

    if serviceable_records:
        cursor.execute(f"UPDATE serviceable_area_mapping SET audit_log='MIGRATED' WHERE location_id={location_id} AND is_active=1 AND audit_log<>'MIGRATED'")
        print(f"Step 5: Serviceable area mapping updated to 'MIGRATED' for location ID {location_id}.")
    else:
        print(f"Step 5: No serviceable area mapping records to update for location ID {location_id}.")

# Step 6: Deactivate pickup_location_configs
def deactivate_pickup_location_configs(cursor, location_id):
    cursor.execute(f"SELECT * FROM pickup_location_configs WHERE location_id={location_id} AND is_active=1 AND flow_type='REVERSE'")
    pickup_location_records = cursor.fetchall()

    if pickup_location_records:
        cursor.execute(f"UPDATE pickup_location_configs SET is_active=0 WHERE location_id={location_id} AND is_active=1 AND flow_type='REVERSE'")
        print(f"Step 6: Rvp configs deactivated for location ID {location_id}.")
    else:
        print(f"Step 6: No Rvp configs to deactivate for location ID {location_id}.")

def process_alias(alias):
    connection = db_connection()
    try:
        with connection.cursor() as cursor:
            # Step 0: Check if alias exists
            cursor.execute(f"SELECT id, pincode_id FROM locations WHERE alias='{alias}' AND status=1 AND entity_type='PARTNER'")
            location = cursor.fetchone()

            if location:
                location_id = location['id']
                pincode_id = location['pincode_id']
                print(f"Location {alias} exists in the system with ID: {location_id}, Pincode ID: {pincode_id}")

                # Step 1: Update NLC to manual
                update_nlc_to_manual(cursor, location_id)

                # Step 2: Deactivate WF config NLC
                deactivate_wf_config(cursor, pincode_id)

                # Step 3: Deactivate network metadata
                deactivate_network_metadata(cursor, alias)

                # Step 4: Update client location name
                update_client_location_name(cursor, alias)

                # Step 5: Update serviceable_area_mapping
                update_serviceable_area_mapping(cursor, location_id)

                # Step 6: Deactivate pickup_location_configs
                deactivate_pickup_location_configs(cursor, location_id)

            else:
                print(f"Location {alias} does not exist in the system.")
        
        connection.commit()
    
    finally:
        connection.close()

# Read CSV and process each alias
def read_and_process_csv(file_path):
    with open(file_path, 'r') as file:
        csv_reader = csv.reader(file)
        rows = list(csv_reader)  # Convert to a list to get the total number of rows
        for index, row in enumerate(rows):
            alias = row[0]  # Assuming there's only one column
            print(f"\nIteration: {index+1}/{len(rows)}")
            print(f"Location: {alias}")
            print("--------")
            process_alias(alias)
            #print("--------")

# Main execution
csv_file_path = 'input.csv'  # Replace with your actual file path
read_and_process_csv(csv_file_path)

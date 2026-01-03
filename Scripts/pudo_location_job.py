import csv
import sys
import pymysql
import logging
import json

# Setup logging
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

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

# DB Config
mysql_config = {
    'host': mysql_host,
    'port': mysql_port,
    'user': mysql_user,
    'password': mysql_password,
    'db': mysql_db,
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}

def validate_headers(headers, expected):
    missing = [col for col in expected if col not in headers]
    if missing:
        logging.error(f"❌ Missing required columns: {missing}")
        sys.exit(1)

def execute_query(query, cursor):
    cursor.execute(query)
    return cursor.fetchall()

def update_location_ops_type(cursor, alias):
    cursor.execute("UPDATE locations SET location_ops_type='PUDO' WHERE alias=%s", (alias,))
    logging.info("✅ Updated location_ops_type to 'PUDO'")

def add_to_branchAppSelfPickupEnabled(cursor, location_id, alias):
    is_present_query = f"""
        SELECT JSON_CONTAINS(config->'$.branchAppSelfPickupEnabledLocations', '{location_id}')
        AS is_present
        FROM application_config
        WHERE id = 1
    """
    cursor.execute(is_present_query)
    result = cursor.fetchone()
    if not result['is_present']:
        update_query = """
            UPDATE application_config
            SET config = JSON_SET(
                config,
                '$.branchAppSelfPickupEnabledLocations',
                JSON_ARRAY_INSERT(
                    COALESCE(JSON_EXTRACT(config, '$.branchAppSelfPickupEnabledLocations'), JSON_ARRAY()),
                    '$[0]',
                    %s
                )
            )
            WHERE id = 1;
        """
        cursor.execute(update_query, (location_id,))
        logging.info(f"✅ Appended {location_id} to branchAppSelfPickupEnabledLocations")
    else:
        logging.info(f"ℹ️ {location_id} already present in branchAppSelfPickupEnabledLocations")

def update_default_user(cursor, location_id):
    cursor.execute(
        "SELECT id FROM users WHERE location_id=%s AND user_level='ADMIN' AND status=1",
        (location_id,)
    )
    user = cursor.fetchone()
    if not user:
        cursor.execute(
            "SELECT entity_id FROM locations WHERE id=%s", (location_id,)
        )
        partner = cursor.fetchone()
        if partner:
            cursor.execute(
                "SELECT id FROM users WHERE partner_id=%s AND user_level='ADMIN' AND status=1",
                (partner['entity_id'],)
            )
            user = cursor.fetchone()
    if user:
        cursor.execute("UPDATE locations SET default_user_id=%s WHERE id=%s", (user['id'], location_id))
        logging.info("✅ Updated default_user_id")
    else:
        logging.warning("⚠️ No valid admin user found")

def update_address_latlong(cursor, address_id, lat, lon):
    cursor.execute("UPDATE addresses SET latitude=%s, longitude=%s WHERE id=%s", (lat, lon, address_id))
    logging.info(f"✅ Updated latitude/longitude for address {address_id}")

def add_or_update_drs_config(cursor, location_id, location_code, alias):
    client_id = 10823
    config_key = 'isAutoCloseDRSEnabledForBranchApp'
    config_json_path = f'$.{config_key}'

    # Step 1: Fetch existing config
    cursor.execute(
        "SELECT config FROM client_location_configs WHERE client_id=%s AND location_id=%s",
        (client_id, location_id)
    )
    row = cursor.fetchone()

    if not row:
        # No existing record — insert fresh one
        new_config = {config_key: True}
        insert_query = """
            INSERT INTO client_location_configs
            (client_id, location_id, client_location_code, config)
            VALUES (%s, %s, %s, %s)
        """
        cursor.execute(insert_query, (client_id, location_id, location_code, json.dumps(new_config)))
        logging.info(f"✅ Inserted DRS config for alias: {alias}")
        return

    raw_config = row['config']

    if not raw_config or raw_config.strip() == '':
        # Config is NULL or empty string: treat as new JSON
        new_config = {config_key: True}
        cursor.execute(
            """
            UPDATE client_location_configs
            SET config = %s
            WHERE client_id = %s AND location_id = %s
            """,
            (json.dumps(new_config), client_id, location_id)
        )
        logging.info(f"✅ Initialized blank config with DRS for alias: {alias}")
        return

    # Step 2: Check if key already exists
    cursor.execute(
        f"""
        SELECT JSON_CONTAINS_PATH(config, 'one', '{config_json_path}') AS is_present
        FROM client_location_configs
        WHERE client_id = %s AND location_id = %s
        """,
        (client_id, location_id)
    )
    is_present = cursor.fetchone()

    if is_present and not is_present['is_present']:
        # Key missing — update config to add it
        cursor.execute(
            f"""
            UPDATE client_location_configs
            SET config = JSON_SET(config, '{config_json_path}', true)
            WHERE client_id = %s AND location_id = %s
            """,
            (client_id, location_id)
        )
        logging.info(f"✅ Updated existing DRS config for alias: {alias}")
    else:
        logging.info(f"ℹ️ DRS config already present for alias: {alias}")


def update_network_metadata(cursor, alias, sc_full):
    cursor.execute("SELECT id FROM network_metadata WHERE next_location_alias=%s AND is_active=1", (alias,))
    rows = cursor.fetchall()
    if rows:
        cursor.execute("UPDATE network_metadata SET location_alias=%s WHERE next_location_alias=%s AND is_active=1",
                       (sc_full, alias))
        logging.info("✅ Updated network_metadata")
    else:
        logging.info("ℹ️ No matching network_metadata found")

def process_row(cursor, row_num, row):
    alias = row['Location Name'].strip()
    lat = row['Lat'].strip()
    lon = row['Long'].strip()
    sc = row['SC'].strip()

    logging.info(f"\n➡️ Processing Row #{row_num}: {row}")

    # Get required data in one go
    cursor.execute("""
        SELECT id, client_location_name, address_id 
        FROM locations 
        WHERE alias=%s
    """, (alias,))
    loc = cursor.fetchone()
    if not loc:
        logging.error("❌ Location not found. Skipping row.")
        return

    location_id = loc['id']
    location_code = loc['client_location_name']
    address_id = loc['address_id']

    # Get SC full alias once
    cursor.execute("SELECT alias FROM locations WHERE client_location_name=%s", (sc,))
    sc_result = cursor.fetchone()
    if not sc_result:
        logging.error(f"❌ Full SC alias not found for {sc}. Skipping row.")
        return
    sc_full = sc_result['alias']

    update_location_ops_type(cursor, alias)
    add_to_branchAppSelfPickupEnabled(cursor, location_id, alias)
    update_default_user(cursor, location_id)
    update_address_latlong(cursor, address_id, lat, lon)
    add_or_update_drs_config(cursor, location_id, location_code, alias)
    update_network_metadata(cursor, alias, sc_full)

def main():
    input_file = 'locations.csv'
    expected_cols = {'Location Name', 'Lat', 'Long', 'SC'}

    try:
        with open(input_file, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            validate_headers(reader.fieldnames, expected_cols)

            conn = pymysql.connect(**mysql_config)
            with conn:
                with conn.cursor() as cursor:
                    for i, row in enumerate(reader, 1):
                        try:
                            process_row(cursor, i, row)
                        except Exception as e:
                            logging.exception(f"❌ Error processing row #{i}")
                    conn.commit()
            logging.info("🎉 Script completed.")
    except FileNotFoundError:
        logging.error(f"CSV file '{input_file}' not found.")
    except Exception as e:
        logging.exception("❌ Unexpected error during execution")

if __name__ == '__main__':
    main()

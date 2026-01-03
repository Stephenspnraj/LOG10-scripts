import os
import csv
import requests
import json
import pymysql
import logging
from colorama import init, Fore, Style

init(autoreset=True)
#meesho-api-staging.loadshare.net/
HOST_URL = os.getenv("HOST_URL", "https://log10-api.loadshare.net")
# HOST_URL = os.getenv("HOST_URL", "https://meesho-api-staging.loadshare.net")


# Setup logging
logging.basicConfig(level=logging.INFO, format="%(message)s")

def get_log10_db_config():
    return dict(
        host=os.getenv("LOG10_DB_HOST", "log10-replica-replica.cco3osxqlq4g.ap-south-1.rds.amazonaws.com"),
        port=int(os.getenv("LOG10_DB_PORT", "3306")),
        user=os.getenv("LOG10_DB_USER", "log10_scripts"),
        password=os.getenv("LOG10_DB_PASS", "D2lx7Wz0Wm9ISAY-Vp1gxKDTzLRRl5k1m"),
        db="loadshare",
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
    )

def get_titan_db_config():
    # You may need to adjust if titan DB credentials differ
    return dict(
        host=os.getenv("TITAN_DB_HOST", "prod-titan-rds-read-replica-3.cco3osxqlq4g.ap-south-1.rds.amazonaws.com"),
        port=int(os.getenv("TITAN_DB_PORT", "3306")),
        user=os.getenv("TITAN_DB_USER", "hermes_scripts"),
        password=os.getenv("TITAN_DB_PASS", "7Xc8redscriptsicsEagQ"),
        db="titan",
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
    )

# def get_log10_db_config():
#     # Returns a dictionary for pymysql.connect for log10
#     return dict(
#         host=os.getenv("LOG10_DB_HOST", "127.0.0.1"),
#         port=int(os.getenv("LOG10_DB_PORT", "3307")),
#         user=os.getenv("LOG10_DB_USER", "log10_staging"),
#         password=os.getenv("LOG10_DB_PASS", "A_edjsHKmDF6vajhL4go6ekP"),
#         db="loadshare",
#         charset="utf8mb4",
#         cursorclass=pymysql.cursors.DictCursor,
#         autocommit=False,
#     )


# def get_titan_db_config():
#     return dict(
#         host=os.getenv("TITAN_DB_HOST", "127.0.0.1"),
#         port=int(os.getenv("TITAN_DB_PORT", "3308")),
#         user=os.getenv("TITAN_DB_USER", "titan_scripts"),
#         password=os.getenv("TITAN_DB_PASS", "titan_scripts"),
#         db="titan",
#         charset="utf8mb4",
#         cursorclass=pymysql.cursors.DictCursor,
#         autocommit=False,
#     )
    
def execute_query(query, conn, args=None):
    with conn.cursor() as cursor:
        cursor.execute(query, args or ())
        return cursor.fetchall()
    conn.commit()

def login_api():
    login_payload = {
        "username": "vineeth.lsn",
        "password": "12345"
    }
    headers = {
        'Content-Type': 'application/json',
        'deviceId': '123123123123'
    }
    response = requests.post(f'{HOST_URL}/v1/login', headers=headers, json=login_payload)
    if response.status_code == 200:
        login_response = response.json()
        token = login_response['response']['token']['accessToken']
        tokenId = login_response['response']['token']['tokenId']
        return token, tokenId
    else:
        logging.error("%sLogin failed. Please check your credentials.%s", Fore.RED, Style.RESET_ALL)
        return None, None

def check_migrated_location(val):
    return str(val).strip().lower() in ("true", "1", "yes")

def existing_location_onboarding_api(token, tokenId, partner_id, fmcode, client_location_name, fmcodeaddress, loc_zipcode, pickup_pincodes, city_id, fmsc, is_migrated_location):
    headers = {
        'token': token,
        'tokenId': tokenId,
        'Content-Type': 'application/json'
    }
    pickup_pincodes_list = [int(pincode.strip()) for pincode in pickup_pincodes.split(',') if pincode.strip()]
    payload = {
        "partner": {
            "id": partner_id
        },
        "location": [
            {
                "name": fmcode,
                "clientLocationName": client_location_name,
                "addressText": fmcodeaddress,
                "locPincode": int(loc_zipcode),
                "staffPayCityId": int(city_id),
                "locationOnboardingOpsType": "FM",
                "isValmoLocation": True,
                "customer": [
                    {
                        "pickupPincodes": pickup_pincodes_list,
                        "id": 10823,
                        "isSelfServed": True
                    }
                ],
                "migratedLocation": check_migrated_location(is_migrated_location)
            }
        ]
    }
    return requests.post(f'{HOST_URL}/b2c/v1/entity-onboarding/partner-location', headers=headers, json=payload)

def existing_location_onboarding_api_bu(token, tokenId, partner_id, branch_admin_name, contact_number, email_id, fmcode, client_location_name, fmcodeaddress, loc_zipcode, pickup_pincodes, city_id, fmsc, is_migrated_location):
    headers = {
        'token': token,
        'tokenId': tokenId,
        'Content-Type': 'application/json'
    }
    pickup_pincodes_list = [int(pincode.strip()) for pincode in pickup_pincodes.split(',') if pincode.strip()]
    payload = {
        "partner": {
            "id": partner_id
        },
        "branchUser": {
            "name": branch_admin_name,
            "contactNumber": int(contact_number) if contact_number else None,
            "email": email_id
        },
        "location": [
            {
                "name": fmcode,
                "clientLocationName": client_location_name,
                "addressText": fmcodeaddress,
                "locPincode": int(loc_zipcode),
                "staffPayCityId": int(city_id),
                "locationOnboardingOpsType": "FM",
                "isValmoLocation": True,
                "customer": [
                    {
                        "pickupPincodes": pickup_pincodes_list,
                        "id": 10823,
                        "isSelfServed": True
                    }
                ],
                "migratedLocation": check_migrated_location(is_migrated_location)
            }
        ]
    }
    return requests.post(f'{HOST_URL}/b2c/v1/entity-onboarding/partner-location', headers=headers, json=payload)

def metadata_insert(fmcode, fmsc, conn):
    try:
        with conn.cursor() as cursor:
            location_query = f"SELECT id FROM locations WHERE (alias = %s OR alias = %s) AND status = 1 and entity_type='PARTNER'"
            location_result = execute_query(location_query, conn, (fmcode, fmsc))
            if location_result and len(location_result) == 2:
                location_alias = fmcode
                next_location_alias = f"{fmsc}.FMSC"
                check_query = (
                    f"SELECT COUNT(*) as cnt FROM loadshare.network_metadata WHERE is_active=1 "
                    f"AND location_alias = %s AND next_location_alias = %s"
                )
                metadata_exists = execute_query(check_query, conn, (location_alias, next_location_alias))
                count = metadata_exists[0]['cnt']
                if count == 0:
                    insert_query = (
                        f"INSERT INTO loadshare.network_metadata "
                        f"(location_alias, next_location_alias, is_active, audit_log) VALUES(%s, %s, 1, 'fm_onboarding')"
                    )
                    cursor.execute(insert_query, (location_alias, next_location_alias))
                    logging.info("%snetwork metadata inserted: %s => %s%s", Fore.GREEN, location_alias, next_location_alias, Style.RESET_ALL)
                    conn.commit()
                else:
                    logging.info("network metadata skipped")
            else:
                logging.warning("Network metadata skipped: Location not found for fmcode: %s or fmsc: %s", fmcode, fmsc)
    except Exception as e:
        logging.error("Error in metadata_insert: %s", e)

def route_creation(fmcode, fmsc, conn):
    try:
        with conn.cursor() as cursor:
            location_query = f"SELECT id FROM locations WHERE (alias = %s OR alias = %s) AND status = 1 and entity_type='PARTNER'"
            location_result = execute_query(location_query, conn, (fmcode, fmsc))
            if location_result and len(location_result) == 2:
                source_query = (
                    f"SELECT entity_id AS sourcePartnerId, id AS sourceLocationId "
                    f"FROM locations WHERE alias = %s AND status = 1 and entity_type='PARTNER' ")
                destination_query = (
                    f"SELECT id AS destinationLocationId FROM locations WHERE alias = %s AND status = 1 and entity_type='PARTNER' ")
                source_result = execute_query(source_query, conn, (fmcode,))
                destination_result = execute_query(destination_query, conn, (fmsc,))
                if source_result and destination_result:
                    source_partner_id = source_result[0]['sourcePartnerId']
                    source_location_id = source_result[0]['sourceLocationId']
                    destination_location_id = destination_result[0]['destinationLocationId']
                    route_name = f"{fmcode} to {fmsc}"
                    token, tokenId = login_api()
                    if not token:
                        logging.error("%sUnable to re-login for route creation!%s", Fore.RED, Style.RESET_ALL)
                        return
                    route_creation_url = f"{HOST_URL}/b2b/v1/partners/268/routes"
                    payload = {
                        "name": route_name,
                        "path": None,
                        "sourcePartnerId": source_partner_id,
                        "sourceLocationId": source_location_id,
                        "intermediateDestinationIds": [],
                        "transitTime": [2],
                        "eligibleForTrip": True,
                        "routeType": "LINEHAUL",
                        "routeMappingType": None,
                        "destinationLocationId": destination_location_id
                    }
                    headers = {'token': token, 'tokenId': tokenId, 'Content-Type': 'application/json'}
                    response = requests.post(route_creation_url, headers=headers, json=payload)
                    try:
                        response_json = response.json()
                        status_code = response_json['status']['code']
                        message = response_json['status']['message']
                        logging.info("Route: %s Response code: %s, Message: %s",
                                    route_name, status_code, message)
                    except Exception:
                        logging.warning("Failed to parse route creation response.")
                else:
                    logging.info("No matching location for route creation %s->%s", fmcode, fmsc)
            else:
                logging.warning("Route creation skipped: Location not found for fmcode: %s or fmsc: %s", fmcode, fmsc)
    except Exception as e:
        logging.error("Error in route_creation: %s", e)

def update_bypass_inscan_config(fmcode, conn):
    try:
        with conn.cursor() as cursor:
            location_id_query = f"SELECT id AS location_id FROM locations WHERE alias = %s AND status = 1 and entity_type='PARTNER'"
            location_id_result = execute_query(location_id_query, conn, (fmcode,))
            if location_id_result:
                location_id = location_id_result[0]['location_id']
                # Check if already present to avoid duplicates
                is_present_query = (
                    "SELECT JSON_CONTAINS(config->'$.bypassInscanLocationIds', %s) AS is_present "
                    "FROM application_config WHERE id = 1"
                )
                is_present_result = execute_query(is_present_query, conn, (str(location_id),))
                
                if not is_present_result or not list(is_present_result[0].values())[0]:
                    update_query = """
                        UPDATE application_config
                        SET config = JSON_SET(
                            config,
                            '$.bypassInscanLocationIds',
                            JSON_ARRAY_APPEND(
                                COALESCE(JSON_EXTRACT(config, '$.bypassInscanLocationIds'), JSON_ARRAY()),
                                '$',
                                %s
                            )
                        )
                        WHERE id = 1
                    """
                    cursor.execute(update_query, (location_id,))
                    conn.commit()
                    logging.info("%sBypassInscan config updated for location: %s => %s%s",
                                 Fore.GREEN, fmcode, location_id, Style.RESET_ALL)
                else:
                    logging.info("Already present in bypassInscanLocationIds %s", fmcode)
            else:
                logging.warning("BypassInscan config skipped: No location ID found for fmcode: %s", fmcode)
    except Exception as e:
        logging.error("Error in update_bypass_inscan_config: %s", e)


def update_auto_pickup_location_config(fmcode, conn):
    try:
        with conn.cursor() as cursor:
            # Fetch both Partner and Customer location IDs
            # Partner alias is just fmcode, Customer alias is fmcode + " (MEESHO Pickup)"
            customer_alias = f"{fmcode} (MEESHO Pickup)"
            location_id_query = (
                f"SELECT id AS location_id FROM locations "
                f"WHERE (alias = %s OR alias = %s) AND status = 1"
            )
            location_id_result = execute_query(location_id_query, conn, (fmcode, customer_alias))
            
            if location_id_result:
                for row in location_id_result:
                    location_id = row['location_id']
                    # Check if already present
                    is_present_query = (
                        "SELECT JSON_CONTAINS(config->'$.customer_10823.auto_pickup_location', %s) AS is_present "
                        "FROM application_config WHERE id = 1"
                    )
                    is_present_result = execute_query(is_present_query, conn, (str(location_id),))
                    
                    if not is_present_result or not list(is_present_result[0].values())[0]:
                        update_query = """
                            UPDATE application_config
                            SET config = JSON_SET(
                                config,
                                '$.customer_10823.auto_pickup_location',
                                JSON_ARRAY_APPEND(
                                    COALESCE(JSON_EXTRACT(config, '$.customer_10823.auto_pickup_location'), JSON_ARRAY()),
                                    '$',
                                    %s
                                )
                            )
                            WHERE id = 1
                        """
                        cursor.execute(update_query, (location_id,))
                        conn.commit()
                        logging.info("%sAutoPickupLocation config updated for location: %s => %s%s",
                                     Fore.GREEN, fmcode, location_id, Style.RESET_ALL)
                    else:
                        logging.info("Already present in auto_pickup_location %s (ID: %s)", fmcode, location_id)
            else:
                logging.warning("AutoPickupLocation config skipped: No location IDs found for fmcode: %s", fmcode)
    except Exception as e:
        logging.error("Error in update_auto_pickup_location_config: %s", e)

def validate_city_and_org(city_id, titan_conn):
    try:
        with titan_conn.cursor() as cursor:
            city_check_query = "SELECT id FROM cities WHERE id = %s AND is_active = 1"
            org_check_query = "SELECT id FROM organizations WHERE operating_city_id = %s AND is_active = 1 LIMIT 1"
            cursor.execute(city_check_query, (int(city_id),))
            city_result = cursor.fetchone()
            if not city_result:
                return False, f"City ID {city_id} not found or inactive in titan DB"
            cursor.execute(org_check_query, (int(city_id),))
            org_result = cursor.fetchone()
            if not org_result:
                return False, f"No active organization for City ID {city_id} in titan DB"
            return True, ""
    except Exception as e:
        return False, f"Exception during city/org validation: {e}"

def process_input_and_configure_system():
    log10_conf = get_log10_db_config()
    titan_conf = get_titan_db_config()
    input_file = 'existing_partner_input_file.csv'
    output_file = 'existing_partner_output.csv'
    success_count = 0
    fail_count = 0

    try:
        with pymysql.connect(**log10_conf) as conn, \
             pymysql.connect(**titan_conf) as titan_conn, \
             open(input_file, mode='r', newline='', encoding="utf-8") as fin, \
             open(output_file, mode='w', newline='', encoding="utf-8") as fout:

            csv_reader = list(csv.DictReader(fin))
            total_iterations = len(csv_reader)
            if total_iterations == 0:
                logging.warning("Input file is empty.")
                return

            fieldnames = list(csv_reader[0].keys()) + ["onboarding_status", "Remarks"]
            writer = csv.DictWriter(fout, fieldnames=fieldnames)
            writer.writeheader()

            for i, row in enumerate(csv_reader, start=1):
                fmcode = row.get('fmcode', '').strip()
                fmsc = row.get('fmsc', '').strip()
                partner_id = row.get('partner_id', '').strip()
                contact_number = row.get('contactNumber', '').strip()
                branch_admin_name = row.get('branch_admin_name', '').strip()
                email_id = row.get('email', '').strip()
                client_location_name = row.get('clientLocationName', '').strip()
                fmcodeaddress = row.get('fmcodeaddress', '').strip()
                loc_zipcode = row.get('loczipcode', '').strip()
                city_id = row.get('city_id', '').strip()
                pickup_pincodes = row.get('pickupPincodes', '').strip()
                is_migrated_location = row.get('isMigratedLocation', '').strip()

                onboarding_status = ""
                remarks = ""

                print()
                print()
                logging.info("Iteration: %d/%d | Location: %s\n%s", i, total_iterations, fmcode, "-" * 40)

                if not city_id or not city_id.isdigit():
                    onboarding_status = "Failed"
                    remarks = f"Invalid or missing city_id: '{city_id}'"
                    logging.error("%s❌ [FAILED]: %s%s", Fore.RED, remarks, Style.RESET_ALL)
                    fail_count += 1
                    row['onboarding_status'] = onboarding_status
                    row['Remarks'] = remarks
                    writer.writerow(row)
                    continue

                is_city_valid, city_validation_msg = validate_city_and_org(city_id, titan_conn)
                if not is_city_valid:
                    onboarding_status = "Failed"
                    remarks = city_validation_msg
                    logging.error("%s❌ [FAILED]: %s%s", Fore.RED, remarks, Style.RESET_ALL)
                    fail_count += 1
                    row['onboarding_status'] = onboarding_status
                    row['Remarks'] = remarks
                    writer.writerow(row)
                    continue

                # Validation: clientLocationName should be equal to fmcode or contain fmcode or be part of fmcode
                if fmcode != client_location_name and fmcode not in client_location_name and client_location_name not in fmcode:
                    onboarding_status = "Failed"
                    remarks = f"Validation Failed: clientLocationName '{client_location_name}' is not related to fmcode '{fmcode}'"
                    logging.error("%s❌ [FAILED]: %s%s", Fore.RED, remarks, Style.RESET_ALL)
                    fail_count += 1
                    row['onboarding_status'] = onboarding_status
                    row['Remarks'] = remarks
                    writer.writerow(row)
                    continue

                exists_query = "SELECT * FROM locations WHERE client_location_name=%s AND status = 1 and entity_type='PARTNER'"
                exists_result = execute_query(exists_query, conn, (client_location_name,))
                if exists_result:
                    onboarding_status = "Failed"
                    remarks = f"clientLocationName '{client_location_name}' already present in system, row skipped"
                    logging.error("%s❌ [FAILED]: %s%s", Fore.RED, remarks, Style.RESET_ALL)
                    fail_count += 1
                    row['onboarding_status'] = onboarding_status
                    row['Remarks'] = remarks
                    writer.writerow(row)
                    continue

                token, tokenId = login_api()
                if not token:
                    onboarding_status = "Failed"
                    remarks = "Failed to login"
                    logging.error("%s❌ [FAILED]: %s%s", Fore.RED, remarks, Style.RESET_ALL)
                    fail_count += 1
                    row['onboarding_status'] = onboarding_status
                    row['Remarks'] = remarks
                    writer.writerow(row)
                    continue

                # Choose which onboarding API
                if branch_admin_name and contact_number and email_id:
                    response = existing_location_onboarding_api_bu(
                        token, tokenId, partner_id, branch_admin_name, contact_number, email_id,
                        fmcode, client_location_name, fmcodeaddress, loc_zipcode, pickup_pincodes, city_id, fmsc, is_migrated_location
                    )
                    logging.info("called existing_location_onboarding_api_bu")
                else:
                    response = existing_location_onboarding_api(
                        token, tokenId, partner_id, fmcode, client_location_name, fmcodeaddress, loc_zipcode, pickup_pincodes, city_id, fmsc, is_migrated_location
                    )
                    logging.info("called existing_location_onboarding_api")

                if response.status_code == 200:
                    try:
                        response_json = response.json()
                        status_code = response_json.get('status', {}).get('code')
                        message = response_json.get('status', {}).get('message')
                        if status_code == 202 and 'response' in response_json and 'entityDetails' in response_json['response']:
                            entity_details = response_json['response']['entityDetails']
                            location_id = entity_details.get('locationId')
                            onboarding_status = "Success"
                            remarks = f"{location_id}"
                            logging.info("%s✅ [SUCCESS]: Location Onboarded - Location ID: %s%s", Fore.GREEN, location_id, Style.RESET_ALL)
                            success_count += 1

                            # Use fresh connections for DB updates
                            with pymysql.connect(**get_log10_db_config()) as write_conn:
                                metadata_insert(fmcode, fmsc, write_conn)
                                update_bypass_inscan_config(fmcode, write_conn)
                                update_auto_pickup_location_config(fmcode, write_conn)
                                route_creation(fmcode, fmsc, write_conn)
                        else:
                            onboarding_status = "Failed"
                            console_msg = f"API failure: status={status_code}, msg={message}"
                            remarks = str(message)
                            logging.error("%s❌ [FAILED]: %s%s", Fore.RED, console_msg, Style.RESET_ALL)
                            fail_count += 1
                    except Exception as e:
                        onboarding_status = "Failed"
                        console_msg = f"Exception while decoding response: {e}"
                        remarks = console_msg
                        logging.error("%s❌ [FAILED]: %s%s", Fore.RED, console_msg, Style.RESET_ALL)
                        fail_count += 1
                else:
                    onboarding_status = "Failed"
                    try:
                        error_json = response.json()
                        message = error_json.get('status', {}).get('message') or error_json.get('message') or f"Partner location API failed: HTTP {response.status_code}"
                    except Exception:
                        message = f"Partner location API failed: HTTP {response.status_code}"
                    logging.error("%s❌ [FAILED]: %s%s", Fore.RED, message, Style.RESET_ALL)
                    remarks = message
                    fail_count += 1

                row['onboarding_status'] = onboarding_status
                row['Remarks'] = remarks
                writer.writerow(row)

        logging.info("\nBatch processing complete. Output written to: %s", output_file)
        logging.info("\n%s📊 Summary:%s", Fore.MAGENTA, Style.RESET_ALL)
        logging.info(" %s✅ Success : %d%s", Fore.GREEN, success_count, Style.RESET_ALL)
        logging.info(" %s❌ Failed : %d%s", Fore.RED, fail_count, Style.RESET_ALL)
        print()

    except pymysql.err.OperationalError as e:
        logging.error("\n%s❌ Database Connection Error: Could not connect to the database.%s\nError Details: %s", Fore.RED, Style.RESET_ALL, e)
    except Exception as e:
        logging.error("\n%s❌ Unexpected Error:%s", Fore.RED, Style.RESET_ALL)
        logging.error(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    process_input_and_configure_system()

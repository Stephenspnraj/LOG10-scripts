import os
import csv
import requests
import json
import pymysql
import logging
from colorama import init, Fore, Style

init(autoreset=True)

HOST_URL = "https://log10-api.loadshare.net"
#HOST_URL = "https://meesho-api-staging.loadshare.net"

# Setup logging
#logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logging.basicConfig(level=logging.INFO, format="%(message)s")


def get_log10_db_config():
    # Returns a dictionary for pymysql.connect for log10
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


def execute_query(query, conn, args=None):
    with conn.cursor() as cursor:
        cursor.execute(query, args or ())
        result = cursor.fetchall()
    conn.commit() 
    return result


def check_loadshare_partner(val):
    return str(val).strip().lower() in ("true", "1", "yes")

def check_migrated_location(val):
    return str(val).strip().lower() in ("true", "1", "yes")


def login_api():
    login_payload = {
        "username": "vineeth.lsn",
        "password": "12345",
    }
    headers = {'Content-Type': 'application/json', 'deviceId': '123123123123'}
    response = requests.post(f'{HOST_URL}/v1/login', headers=headers, json=login_payload)
    if response.status_code == 200:
        login_response = response.json()
        token = login_response['response']['token']['accessToken']
        tokenId = login_response['response']['token']['tokenId']
        return token, tokenId
    else:
        logging.error("%sLogin failed. Please check your credentials.%s", Fore.RED, Style.RESET_ALL)
        return None, None


def partner_location_api(token, tokenId, partner_name, contact_number, branch_admin_name,
                        email_id, dccode, client_location_name, dc_address,
                        loc_zipcode, delivery_pincodes, city_id, sc, is_loadshare_partner,is_migrated_location):
    headers = {
        'token': token,
        'tokenId': tokenId,
        'Content-Type': 'application/json'
    }
    delivery_pincodes_list = [int(p.strip()) for p in delivery_pincodes.split(',') if p.strip()]
    payload = {
        "partner": {
            "name": partner_name,
            "contactNumber": int(contact_number),
            "isLoadsharePartner": check_loadshare_partner(is_loadshare_partner)
        },
        "user": {
            "name": branch_admin_name,
            "contactNumber": int(contact_number),
            "emailId": email_id
        },
        "location": [
            {
                "name": dccode,
                "clientLocationName": client_location_name,
                "addressText": dc_address,
                "locPincode": int(loc_zipcode),
                "staffPayCityId": int(city_id),
                "locationOnboardingOpsType": "LM",
                "isValmoLocation": True,
                "deliveryPincodes": delivery_pincodes_list,
                "rvpCustomer": [
                    {
                        "pickupPincodes": delivery_pincodes_list,
                        "id": 10823,
                        "isSelfServed": False
                    }
                ],
                "migratedLocation": check_migrated_location(is_migrated_location)
            }
        ]
    }
    response = requests.post(f'{HOST_URL}/b2c/v1/entity-onboarding/partner-location', headers=headers, json=payload)
    return response


def metadata_insert(dccode, sc, conn):
    try:
        with conn.cursor() as cursor:
            location_query = f"SELECT id FROM locations WHERE (alias = %s OR alias = %s) AND status = 1 and entity_type='PARTNER'"
            location_result = execute_query(location_query, conn, (dccode, sc))
            #print(location_result)
            if location_result and len(location_result) == 2:
                location_alias = f"{sc}.LMSC"
                next_location_alias = dccode
                check_query = (
                    f"SELECT COUNT(*) as cnt FROM loadshare.network_metadata WHERE is_active=1 "
                    f"AND location_alias = %s AND next_location_alias = %s"
                )
                metadata_exists = execute_query(check_query, conn, (location_alias, next_location_alias))
                count = metadata_exists[0]['cnt']
                if count == 0:
                    insert_query = (
                        f"INSERT INTO loadshare.network_metadata "
                        f"(location_alias, next_location_alias, is_active) VALUES(%s, %s, 1)"
                    )
                    cursor.execute(insert_query, (location_alias, next_location_alias))
                    logging.info("%snetwork metadata inserted: %s => %s%s", Fore.GREEN, location_alias, next_location_alias, Style.RESET_ALL)
                    conn.commit()
                else:
                    logging.info("network metadata skipped")
            else:
                logging.warning("Network metadata skipped: Location not found for dccode: %s or sc: %s", dccode, sc)
    except Exception as e:
        logging.error("Error in metadata_insert: %s", e)


def route_creation(dccode, sc, conn):
    try:
        with conn.cursor() as cursor:
            location_query = f"SELECT id FROM locations WHERE (alias = %s OR alias = %s) AND status = 1 and entity_type='PARTNER'"
            location_result = execute_query(location_query, conn, (dccode, sc))
            #print(location_result)
            if location_result and len(location_result) == 2:
                source_query = (
                    f"SELECT entity_id AS sourcePartnerId, id AS sourceLocationId "
                    f"FROM locations WHERE alias = %s AND status = 1 and entity_type='PARTNER' ")
                destination_query = (
                    f"SELECT id AS destinationLocationId FROM locations WHERE alias = %s AND status = 1 and entity_type='PARTNER' ")
                source_result = execute_query(source_query, conn, (dccode,))
                destination_result = execute_query(destination_query, conn, (sc,))
                if source_result and destination_result:
                    source_partner_id = source_result[0]['sourcePartnerId']
                    source_location_id = source_result[0]['sourceLocationId']
                    destination_location_id = destination_result[0]['destinationLocationId']
                    route_name = f"{dccode} to {sc}"
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
                    logging.info("No matching location for route creation %s->%s", dccode, sc)
            else:
                logging.warning("Route creation skipped: Location not found for dccode: %s or sc: %s", dccode, sc)
    except Exception as e:
        logging.error("Error in route_creation: %s", e)


def update_wrong_facility_scan(dccode, conn):
    try:
        with conn.cursor() as cursor:
            location_id_query = f"SELECT id AS location_id FROM locations WHERE alias = %s AND status = 1 and entity_type='PARTNER'"
            location_id_result = execute_query(location_id_query, conn, (dccode,))
            #print(location_id_result)
            if location_id_result:
                location_id = location_id_result[0]['location_id']
                is_present_query = (
                    "SELECT JSON_CONTAINS(config->'$.enableWrongFacilityShipmentScan', %s) AS is_present "
                    "FROM application_config WHERE id = 1"
                )
                is_present_result = execute_query(is_present_query, conn, (str(location_id),))
                if not list(is_present_result[0].values())[0]:
                    update_query = """
                        UPDATE application_config
                        SET config = JSON_SET(
                            config, '$.enableWrongFacilityShipmentScan',
                            JSON_ARRAY_INSERT(
                                COALESCE(JSON_EXTRACT(config, '$.enableWrongFacilityShipmentScan'), JSON_ARRAY()),
                                '$[0]', %s )
                        ) WHERE id = 1;
                    """
                    cursor.execute(update_query, (location_id,))
                    conn.commit()
                    logging.info("%sWrongFacility updated for location: %s => %s%s",
                                 Fore.GREEN, dccode, location_id, Style.RESET_ALL)
                else:
                    logging.info("Already present in enableWrongFacilityShipmentScan %s", dccode)
            else:
                logging.warning("wrong_facility config skipped: No location ID found for dccode: %s", dccode)
    except Exception as e:
        logging.error("Error in update_wrong_facility_scan: %s", e)


def update_call_masking_exclude_location(dccode, conn):
    try:
        with conn.cursor() as cursor:
            location_id_query = f"SELECT id AS location_id FROM locations WHERE alias = %s AND status = 1 and entity_type='PARTNER'"
            location_id_result = execute_query(location_id_query, conn, (dccode,))
            #print(location_id_result)
            if location_id_result:
                location_id = location_id_result[0]['location_id']
                is_present_query = (
                    "SELECT JSON_CONTAINS(config->'$.call_masking_exclude_location', %s) AS is_present "
                    "FROM application_config WHERE id = 1"
                )
                is_present_result = execute_query(is_present_query, conn, (str(location_id),))
                if not list(is_present_result[0].values())[0]:
                    update_query = """
                        UPDATE application_config
                        SET config = JSON_SET(
                            config, '$.call_masking_exclude_location',
                            JSON_ARRAY_INSERT(
                                COALESCE(JSON_EXTRACT(config, '$.call_masking_exclude_location'), JSON_ARRAY()),
                                '$[0]', %s )
                        ) WHERE id = 1;
                    """
                    cursor.execute(update_query, (location_id,))
                    conn.commit()
                    logging.info(
                        "%sCall masking exclusion updated for location: %s => %s%s",
                        Fore.GREEN, dccode, location_id, Style.RESET_ALL)
                else:
                    logging.info("Already present in call_masking_exclude_location %s", dccode)
            else:
                logging.warning("Call masking exclusion config skipped: No location ID found for dccode: %s", dccode)
    except Exception as e:
        logging.error("Error in update_call_masking_exclude_location: %s", e)


def validate_city_and_org(city_id, titan_conn):
    try:
        with titan_conn.cursor() as cursor:
            city_check_query = f"SELECT id FROM cities WHERE id = %s AND is_active = 1"
            org_check_query = ("SELECT id FROM organizations WHERE operating_city_id = %s AND is_active = 1 LIMIT 1")
            cursor.execute(city_check_query, (int(city_id), ))
            city_result = cursor.fetchone()
            if not city_result:
                return False, f"City ID {city_id} not found or inactive in titan DB"
            cursor.execute(org_check_query, (int(city_id), ))
            org_result = cursor.fetchone()
            if not org_result:
                return False, f"No active organization for City ID {city_id} in titan DB"
            return True, ""
    except Exception as e:
        return False, f"Exception during city/org validation: {e}"


def process_input_and_configure_system():
    mysql_conf = get_log10_db_config()
    titan_mysql_conf = get_titan_db_config()
    input_file = 'new_partner_input_file.csv'
    output_file = 'new_partner_output.csv'
    success_count = 0
    fail_count = 0

    with pymysql.connect(**mysql_conf) as conn, \
         pymysql.connect(**titan_mysql_conf) as titan_conn, \
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
            dccode = row.get('dccode', '').strip()
            sc = row.get('sc', '').strip()
            partner_name = row.get('Partner_name', '').strip()
            contact_number = row.get('contactNumber', '').strip()
            branch_admin_name = row.get('branch_admin_name', '').strip()
            email_id = row.get('email', '').strip()
            client_location_name = row.get('clientLocationName', '').strip()
            dc_address = row.get('dcaddress', '').strip()
            loc_zipcode = row.get('loczipcode', '').strip()
            delivery_pincodes = row.get('deliveryPincodes', '').strip()
            city_id = row.get('city_id', '').strip()
            is_loadshare_partner = row.get('isLoadsharePartner', '').strip()
            is_migrated_location = row.get('isMigratedLocation', '').strip()
            onboarding_status = ""
            remarks = ""

            print()
            logging.info("Iteration: %d/%d | Location: %s\n%s", i, total_iterations, dccode, '-'*40)
            #print()

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

            response = partner_location_api(
                token, tokenId, partner_name, contact_number, branch_admin_name, email_id,
                dccode, client_location_name, dc_address, loc_zipcode, delivery_pincodes, city_id,
                sc, is_loadshare_partner,is_migrated_location
            )

            if response.status_code == 200:
                try:
                    response_json = response.json()
                    status_code = response_json['status']['code']
                    message = response_json['status']['message']
                    if status_code == 202 and 'response' in response_json and 'entityDetails' in response_json['response']:
                        entity_details = response_json['response']['entityDetails']
                        location_id = entity_details.get('locationId')
                        onboarding_status = "Success"
                        remarks = f"{location_id}"
                        logging.info("%s✅ [SUCCESS]: Location Onboarded - Location ID: %s%s", Fore.GREEN, location_id, Style.RESET_ALL)
                        success_count += 1

                        # Use a separate connection for atomic DB updates for each successful row
                        with pymysql.connect(**mysql_conf) as write_conn:
                            metadata_insert(dccode, sc, write_conn)
                            update_wrong_facility_scan(dccode, write_conn)
                            update_call_masking_exclude_location(dccode, write_conn)
                            route_creation(dccode, sc, write_conn)
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
                console_msg = message
                logging.error("%s❌ [FAILED]: %s%s", Fore.RED, console_msg, Style.RESET_ALL)
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


if __name__ == "__main__":
    process_input_and_configure_system()

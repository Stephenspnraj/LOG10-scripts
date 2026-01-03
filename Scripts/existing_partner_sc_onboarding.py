import os
import csv
import requests
import json
import pymysql
import logging
from colorama import init, Fore, Style

init(autoreset=True)

#HOST_URL = "https://meesho-api-staging.loadshare.net"
HOST_URL = "https://log10-api.loadshare.net"
# Setup logging
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
        return cursor.fetchall()


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


def partner_location_api(token, tokenId, partner_id, sc_code, client_location_name, sc_address, loc_pincode, city_id, gst_number):
    headers = {
        'token': token,
        'tokenId': tokenId,
        'Content-Type': 'application/json'
    }
    payload = {
        "partner": {
            "id": int(partner_id)
        },
        "location": [
            {
                "name": sc_code,
                "clientLocationName": client_location_name,
                "addressText": sc_address,
                "locPincode": int(loc_pincode),
                "staffPayCityId": int(city_id),
                "locationOnboardingOpsType": "SC",
                "isValmoLocation": True,
                "gstNumber": gst_number
            }
        ]
    }
    response = requests.post(f'{HOST_URL}/b2c/v1/entity-onboarding/partner-location', headers=headers, json=payload)
    return response


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
    input_file = 'existing_partner_sc_onboarding_input.csv'
    output_file = 'existing_partner_sc_onboarding_output.csv'
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
            sc_code = row.get('sc_code', '').strip()
            partner_id = row.get('partner_id', '').strip()
            client_location_name = row.get('clientLocationName', '').strip()
            sc_address = row.get('scaddress', '').strip()
            loc_pincode = row.get('locpincode', '').strip()
            city_id = row.get('city_id', '').strip()
            gst_number = row.get('gst_number', '').strip()
            onboarding_status = ""
            remarks = ""

            print()
            logging.info("Iteration: %d/%d | Location: %s\n%s", i, total_iterations, sc_code, '-'*40)

            # Validate sc_code and clientLocationName
            if not sc_code or not client_location_name:
                onboarding_status = "Failed"
                remarks = "sc_code or clientLocationName is missing"
                logging.error("%s❌ [FAILED]: %s%s", Fore.RED, remarks, Style.RESET_ALL)
                fail_count += 1
                row['onboarding_status'] = onboarding_status
                row['Remarks'] = remarks
                writer.writerow(row)
                continue
            if not (sc_code.lower() == client_location_name.lower() or client_location_name.lower() in sc_code.lower()):
                onboarding_status = "Failed"
                remarks = f"clientLocationName '{client_location_name}' must match or be a substring of sc_code '{sc_code}'"
                logging.error("%s❌ [FAILED]: %s%s", Fore.RED, remarks, Style.RESET_ALL)
                fail_count += 1
                row['onboarding_status'] = onboarding_status
                row['Remarks'] = remarks
                writer.writerow(row)
                continue

            # Validate partner_id
            if not partner_id or not partner_id.isdigit():
                onboarding_status = "Failed"
                remarks = f"Invalid or missing partner_id: '{partner_id}'"
                logging.error("%s❌ [FAILED]: %s%s", Fore.RED, remarks, Style.RESET_ALL)
                fail_count += 1
                row['onboarding_status'] = onboarding_status
                row['Remarks'] = remarks
                writer.writerow(row)
                continue

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
                token, tokenId, partner_id, sc_code, client_location_name, sc_address, loc_pincode, city_id, gst_number
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
    process_input_and_configure_system()existing_partner_sc_onboarding.py

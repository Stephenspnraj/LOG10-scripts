import csv
import pymysql
import requests
import subprocess
from tabulate import tabulate
from colorama import init, Fore, Style

# Database credentials
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

# Login API credentials
# LOGIN_URL = 'https://meesho-api-staging.loadshare.net/v1/login'
# ROUTE_CREATION_URL = 'https://meesho-api-staging.loadshare.net/b2b/v1/partners/268/routes'
# USERNAME = 'vineeth.lsn'
# PASSWORD = '12345'
# DEVICE_ID = '123123123123'

# Login API credentials
LOGIN_URL = 'https://log10-api.loadshare.net/v1/login'
ROUTE_CREATION_URL = 'https://log10-api.loadshare.net/b2b/v1/partners/268/routes'
USERNAME = 'vineeth.lsn'
PASSWORD = '12345'
DEVICE_ID = '123123123123'

# Required CSV headers
REQUIRED_HEADERS = [
    'LMDC', 'Current Sort Centre', 'New Sort Centre',
    'Current Sort Code', 'New Sort Code'
]

def get_mysql_connection():
    return pymysql.connect(
        host=mysql_host,
        port=mysql_port,
        user=mysql_user,
        password=mysql_password,
        db=mysql_db,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

def get_token():
    try:
        login_headers = {
            'Content-Type': 'application/json',
            'deviceId': DEVICE_ID
        }
        login_data = {
            "username": USERNAME,
            "password": PASSWORD
        }
        response = requests.post(LOGIN_URL, headers=login_headers, json=login_data)
        response.raise_for_status()
        login_response = response.json()
        token = login_response['response']['token']['accessToken']
        tokenId = login_response['response']['token']['tokenId']
        return token, tokenId
    except Exception as e:
        print(f"Login failed: {e}")
        return None, None


def get_location_id(cursor, column, value):
    cursor.execute(f"SELECT id FROM locations WHERE {column} = %s", (value,))
    result = cursor.fetchone()
    return result["id"] if result else None

def print_query_result(cursor, query, params=None):
    cursor.execute(query, params or ())
    rows = cursor.fetchall()
    if rows:
        print(tabulate(rows, headers="keys", tablefmt="pretty"))
    else:
        print("No results found.")

def run_curl_route_creation(source_id, dest_id, name, token, tokenId):
    payload = {
        "name": name,
        "path": None,
        "sourceLocationId": source_id,
        "intermediateDestinationIds": [],
        "transitTime": [1],
        "eligibleForTrip": True,
        "routeType": "LINEHAUL",
        "routeMappingType": None,
        "destinationLocationId": dest_id
    }

    headers = {
        "Content-Type": "application/json",
        "token": token,
        "tokenId": tokenId
    }

    response = requests.post(ROUTE_CREATION_URL, headers=headers, json=payload)
    #print("Route creation response:", response.status_code, response.text)
    route_creation_response = response.json()
    status_code = route_creation_response['status']['code']
    message = route_creation_response['status']['message']
    #print(f"Route creation response: status_code:{status_code} \"message\":\"{message}\"\n")
    print(f"Route creation ({name}), response:- status_code:{status_code} \"message\":\"{message}\"\n")



def normalize_headers(headers):
    return {h.strip().lower(): h for h in headers}

def process_csv(file_path):
    conn = get_mysql_connection()
    cursor = conn.cursor()
    token, tokenId = get_token()
    if not token or not tokenId:
        print("Token fetch failed. Exiting.")
        return

    with open(file_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        headers_map = normalize_headers(reader.fieldnames)

        for col in REQUIRED_HEADERS:
            if col.lower() not in headers_map:
                print(f"Missing required column: {col}")
                return

        for i, row in enumerate(reader, 1):
            

            #print(f"Processing rrow{i}: [{lmdc},{curr_sort},{new_sort},{curr_code},{new_code}]")

            try:
                lmdc = row[headers_map['lmdc']].strip()
                curr_sort = row[headers_map['current sort centre']].strip()
                new_sort = row[headers_map['new sort centre']].strip()
                curr_code = row[headers_map['current sort code']].strip()
                new_code = row[headers_map['new sort code']].strip()
                #print(f"\nProcessing row {i}: [{lmdc}, {curr_sort}, {new_sort}, {curr_code}, {new_code}]")
                print(Fore.CYAN + f"\nProcessing row {i}: " + Fore.CYAN + f"[{lmdc}, {curr_sort}, {new_sort}, {curr_code}, {new_code}]\n")
                init(autoreset=True)    
                # Step 1: Validation
                lmdc_id = get_location_id(cursor, 'client_location_name', lmdc)
                curr_sort_id = get_location_id(cursor, 'alias', curr_sort)
                new_sort_id = get_location_id(cursor, 'alias', new_sort)

                # if not all([lmdc_id, curr_sort_id, new_sort_id]):
                #     print("Validation failed: One or more location IDs not found.")
                #     continue
                missing = []
                if not lmdc_id:
                    missing.append(f"LMDC ({lmdc})")
                if not curr_sort_id:
                    missing.append(f"Current Sort Centre ({curr_sort})")
                if not new_sort_id:
                    missing.append(f"New Sort Centre ({new_sort})")

                if missing:
                    print(f"Validation failed: Missing locations : {', '.join(missing)}")
                    continue

                # Step 1: network_metadata update
                print("Before Network Metadata Update:")
                print_query_result(cursor, "SELECT id,location_alias,next_location_alias,crossdock_alias,is_active,updated_at FROM network_metadata WHERE next_location_alias IN (SELECT alias FROM locations WHERE client_location_name = %s)", (lmdc,))
                print(f"""Executing query:
                      UPDATE network_metadata SET location_alias = '{new_sort}.LMSC', next_location_alias = '{new_code}'
    WHERE next_location_alias IN (SELECT alias FROM locations WHERE client_location_name = '{lmdc}')
    """)        
                #print_query_result(cursor, "SELECT alias FROM locations WHERE client_location_name = %s)", (lmdc,))
                #print_query_result(cursor, "SELECT id,alias,client_location_name,updated_at FROM locations WHERE client_location_name = %s", (lmdc,))

                cursor.execute("""
                    UPDATE network_metadata
                    SET location_alias = %s, next_location_alias = %s
                    WHERE next_location_alias IN (SELECT alias FROM locations WHERE client_location_name = %s)
                """, (new_sort + ".LMSC", new_code, lmdc))
                conn.commit()
                #print("Updated network_metadata.")

                # Step 2: location update
                init(autoreset=True)
                print("Before Location Update:")
                #print(Fore.YELLOW + "Before Location Update:" + Style.RESET_ALL)
                print_query_result(cursor, "SELECT id,alias,client_location_name,updated_at FROM locations WHERE client_location_name = %s", (lmdc,))
                print(f"Executing query: UPDATE locations SET alias = '{new_code}' WHERE client_location_name = '{lmdc}'")
                cursor.execute("UPDATE locations SET alias = %s WHERE client_location_name = %s", (new_code, lmdc))
                conn.commit()
                #print("Updated location alias.")
                #print_query_result(cursor, "SELECT id,alias,client_location_name,updated_at FROM locations WHERE client_location_name = %s", (lmdc,))


    #             # Step 2: network_metadata update
    #             print("\nBefore Network Metadata Update:")
    #             print_query_result(cursor, "SELECT id,location_alias,next_location_alias,crossdock_alias,is_active,updated_at FROM network_metadata WHERE next_location_alias IN (SELECT alias FROM locations WHERE client_location_name = %s)", (lmdc,))
    #             print(f"""Executing query:
    #                   UPDATE network_metadata SET location_alias = '{new_sort}.LMSC', next_location_alias = '{new_code}'
    # WHERE next_location_alias IN (SELECT alias FROM locations WHERE client_location_name = '{lmdc}')""")        
                # print_query_result(cursor, "SELECT alias FROM locations WHERE client_location_name = %s)", (lmdc,))
                # print_query_result(cursor, "SELECT id,alias,client_location_name,updated_at FROM locations WHERE client_location_name = %s", (lmdc,))

                # cursor.execute("""
                #     UPDATE network_metadata
                #     SET location_alias = %s, next_location_alias = %s
                #     WHERE next_location_alias IN (SELECT alias FROM locations WHERE client_location_name = %s)
                # """, (new_sort + ".LMSC", new_code, lmdc))
                # conn.commit()
                # #print("Updated network_metadata.")

                # Step 3: next_location_configs update
                print("Executing select nlc query from sc-lm:")
                print_query_result(cursor, """
    SELECT
      nlc.id,
      l.alias AS location,
      ln.alias AS next_location_name,
      dl.alias AS dest_loc_name,
      nlc.pincode_id,
      nlc.updated_at,
      nlc.is_manual
    FROM
      next_location_configs nlc
      JOIN locations l ON nlc.location_id = l.id 
      JOIN locations ln ON nlc.next_location_id = ln.id
      JOIN locations dl ON dl.pincode_id = nlc.pincode_id
        AND dl.entity_type = 'PARTNER'
        AND dl.status = 1 
        AND dl.is_valmo_location=1
    WHERE
      nlc.entity_type = 'manifest'
      AND nlc.is_active = 1
      AND nlc.location_id = %s
      AND nlc.next_location_id = %s
      AND nlc.pincode_id IN (
        SELECT pincode_id FROM locations WHERE client_location_name = %s
      )
    ORDER BY 
      nlc.updated_at DESC
""", (curr_sort_id, lmdc_id, lmdc))
                # print("executing sc-lm nlc update query:")
                print(f"""
Executing SC-LM nlc update query:
UPDATE next_location_configs
SET is_manual=1, audit_log='LMSC_Migration'
WHERE entity_type='manifest' AND is_active=1
AND location_id={curr_sort_id} AND next_location_id={lmdc_id}
AND pincode_id IN (SELECT pincode_id FROM locations WHERE client_location_name = '{lmdc}')
""")
                cursor.execute("""
                    UPDATE next_location_configs
                    SET is_manual=1, audit_log='LMSC_Migration'
                    WHERE entity_type='manifest' AND is_active=1
                    AND location_id=%s AND next_location_id=%s
                    AND pincode_id IN (SELECT pincode_id FROM locations WHERE client_location_name = %s)
                """, (curr_sort_id, lmdc_id, lmdc))
                print("executing select query of LM-SC:")
                print_query_result(cursor, """
    SELECT
      nlc.id,
      l.alias AS location,
      ln.alias AS next_location_name,
      dl.alias AS dest_loc_name,
      nlc.pincode_id,
      nlc.updated_at,
      nlc.is_manual
    FROM
      next_location_configs nlc
      JOIN locations l ON nlc.location_id = l.id 
      JOIN locations ln ON nlc.next_location_id = ln.id
      JOIN locations dl ON dl.pincode_id = nlc.pincode_id
        AND dl.entity_type = 'PARTNER'
        AND dl.status = 1 
        AND dl.is_valmo_location=1
    WHERE
      nlc.entity_type = 'manifest'
      AND nlc.is_active = 1
      AND nlc.location_id = %s
      AND nlc.next_location_id = %s
      AND nlc.pincode_id IN (
        SELECT pincode_id FROM locations WHERE alias = %s
      )
    ORDER BY 
      nlc.updated_at DESC
""", (lmdc_id, curr_sort_id, curr_sort))

                #print("executing LM-SC nlc update query:")
                print(f"""
Executing LM-SC nlc update query:
UPDATE next_location_configs
SET is_manual=1, audit_log='LMSC_Migration'
WHERE entity_type='manifest' AND is_active=1
AND location_id={lmdc_id} AND next_location_id={curr_sort_id}
AND pincode_id IN (SELECT pincode_id FROM locations WHERE client_location_name = '{curr_sort}')
""")

                cursor.execute("""
                    UPDATE next_location_configs
                    SET is_manual=1, audit_log='LMSC_Migration'
                    WHERE entity_type='manifest' AND is_active=1
                    AND location_id=%s AND next_location_id=%s
                    AND pincode_id IN (SELECT pincode_id FROM locations WHERE alias = %s)
                """, (lmdc_id, curr_sort_id, curr_sort))
                conn.commit()
                #print("Updated next_location_configs.")

                # Step 4: Route creation
                route_name = f"{new_code} to {new_sort}"
                run_curl_route_creation(lmdc_id, new_sort_id, route_name, token, tokenId)
                    # Commit after all steps succeed
                #conn.commit()
                print(f"✅ Migration successful for {lmdc}")

            except Exception as e:
                #conn.rollback()
                print(f"Error processing row{i}: {e}")
                print(Fore.RED + f"Error processing row {i}: {e}")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    process_csv("LMSCmigration.csv")

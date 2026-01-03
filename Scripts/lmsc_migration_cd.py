import csv
import pymysql
import requests
import subprocess
from tabulate import tabulate
from colorama import init, Fore, Style

# Database credentials
mysql_host = 'log10-replica-replica.cco3osxqlq4g.ap-south-1.rds.amazonaws.com'
mysql_port = 3306  # Assuming default MySQL port
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
    cursor.execute(f"SELECT id FROM locations WHERE entity_type = 'PARTNER' AND status = 1 and {column} = %s", (value,))
    result = cursor.fetchone()
    return result["id"] if result else None

def get_location_alias(cursor, client_location_name):
    cursor.execute("SELECT alias FROM locations WHERE entity_type = 'PARTNER' AND status = 1 and client_location_name = %s", (client_location_name,))
    result = cursor.fetchone()
    return result["alias"] if result else None

def get_pincode_id_by_location_id(cursor, location_id):
    cursor.execute("SELECT pincode_id FROM locations WHERE entity_type = 'PARTNER' AND status = 1 and id = %s", (location_id,))
    result = cursor.fetchone()
    return result["pincode_id"] if result else None

def ensure_next_location_config(cursor, location_id, next_location_id, pincode_id, audit_log='LMSC_Migration'):
    if pincode_id is None:
        return False
    cursor.execute(
        """
        SELECT id FROM next_location_configs
        WHERE entity_type='MANIFEST' AND is_active=1
        AND location_id=%s AND next_location_id=%s AND pincode_id=%s
        """,
        (location_id, next_location_id, pincode_id),
    )
    exists = cursor.fetchone()
    if not exists:
        cursor.execute(
            """
            INSERT INTO next_location_configs
            (location_id, next_location_id, pincode_id, entity_type, is_active, is_manual, audit_log)
            VALUES (%s, %s, %s, 'MANIFEST', 1, 1, %s)
            """,
            (location_id, next_location_id, pincode_id, audit_log),
        )
        return True
    return False

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

    #print(payload)
    response = requests.post(ROUTE_CREATION_URL, headers=headers, json=payload)
    #print("Route creation response:", response.status_code, response.text)
    route_creation_response = response.json()
    status_code = route_creation_response['status']['code']
    message = route_creation_response['status']['message']
    #print(f"Route creation response: status_code:{status_code} \"message\":\"{message}\"\n")
    print(f"Route creation ({name})")
    print(f"Status Code: {status_code}")
    print(f"Message: {message}\n")



def normalize_headers(headers):
    return {h.strip().lower(): h for h in headers}

def process_csv(file_path):
    conn = get_mysql_connection()
    cursor = conn.cursor()
    token, tokenId = get_token()
    if not token or not tokenId:
        print("Token fetch failed. Exiting.")
        return

    skipped_validation = 0
    processed_rows = []
    
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
                print(Fore.CYAN + f"\n{'='*60}")
                print(Fore.CYAN + f"Processing Row {i}: {lmdc}")
                print(Fore.CYAN + f"Current Sort: {curr_sort} -> New Sort: {new_sort}")
                print(Fore.CYAN + f"Current Code: {curr_code} -> New Code: {new_code}")
                print(Fore.CYAN + f"{'='*60}")
                init(autoreset=True)    
                
                remarks = []
                # Validation: LMDC must be present in New Sort Code (case-insensitive)
                if lmdc.lower() not in new_code.lower():
                    msg = f"Validation failed: LMDC ({lmdc}) not in New Sort Code ({new_code})."
                    print(Fore.YELLOW + msg)
                    remarks.append(msg)
                    skipped_validation += 1
                    row['Remarks'] = "; ".join(remarks)
                    processed_rows.append(row)
                    continue
                
                # Step 1: Validation
                print(Fore.GREEN + "\n📋 Step 1: Validating Locations...")
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
                    msg = f"Validation failed: Missing locations : {', '.join(missing)}"
                    print(Fore.YELLOW + msg)
                    remarks.append(msg)
                    skipped_validation += 1
                    row['Remarks'] = "; ".join(remarks)
                    processed_rows.append(row)
                    continue

                print(Fore.GREEN + "✅ All locations validated successfully")

                # Step 2: Update Network Metadata
                print(Fore.GREEN + "\n🔄 Step 2: Updating Network Metadata...")
                print("Before Network Metadata Update:")
                print_query_result(cursor, "SELECT id,location_alias,next_location_alias,crossdock_alias,is_active,updated_at FROM network_metadata WHERE is_active=1 and next_location_alias IN (SELECT alias FROM locations WHERE entity_type = 'PARTNER' AND status = 1 and client_location_name = %s)", (lmdc,))
                #print(f"""Executing query:
                #      UPDATE network_metadata SET location_alias = '{new_sort}.LMSC', next_location_alias = '{new_code}'
                #WHERE next_location_alias IN (SELECT alias FROM locations WHERE client_location_name = '{lmdc}')
                #""")        
                #print_query_result(cursor, "SELECT alias FROM locations WHERE client_location_name = %s)", (lmdc,))
                #print_query_result(cursor, "SELECT id,alias,client_location_name,updated_at FROM locations WHERE client_location_name = %s", (lmdc,))

                cursor.execute("""
                    UPDATE network_metadata
                    SET location_alias = %s, next_location_alias = %s,audit_log='LMSC_Migration'
                    WHERE is_active=1 and next_location_alias IN (SELECT alias FROM locations WHERE entity_type = 'PARTNER' AND status = 1 and client_location_name = %s)
                """, (new_sort + ".LMSC", new_code, lmdc))
                conn.commit()
                print(Fore.GREEN + "✅ Network metadata updated successfully")
                #print("Updated network_metadata.")

                # Step 3: Update Location Alias
                print(Fore.GREEN + "\n🏷️  Step 3: Updating Location Alias...")
                init(autoreset=True)
                print("Before Location Update:")
                #print(Fore.YELLOW + "Before Location Update:" + Style.RESET_ALL)
                print_query_result(cursor, "SELECT id,alias,client_location_name,updated_at FROM locations WHERE entity_type = 'PARTNER' AND status = 1 and client_location_name = %s", (lmdc,))
                #print(f"Executing query: UPDATE locations SET alias = '{new_code}' WHERE client_location_name = '{lmdc}'")
                cursor.execute("UPDATE locations SET alias = %s WHERE entity_type = 'PARTNER' AND status = 1 and client_location_name = %s", (new_code, lmdc))
                conn.commit()
                print(Fore.GREEN + "✅ Location alias updated successfully")
                #print("Updated location alias.")
                #print_query_result(cursor, "SELECT id,alias,client_location_name,updated_at FROM locations WHERE client_location_name = %s", (lmdc,))

                # Step 4: Update Next Location Configs
                print(Fore.GREEN + "\n🔗 Step 4: Updating Next Location Configs...")
                print("Checking SC-LM configurations:")
                sc_lm_results = cursor.execute("""
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
        AND dl.entity_id > 100000
    WHERE
      nlc.entity_type = 'MANIFEST'
      AND nlc.is_active = 1
      AND nlc.location_id = %s
      AND nlc.next_location_id = %s
      AND nlc.pincode_id IN (
        SELECT pincode_id FROM locations WHERE client_location_name = %s
      )
    ORDER BY 
      nlc.updated_at DESC
""", (curr_sort_id, lmdc_id, lmdc))
                sc_lm_rows = cursor.fetchall()
                if sc_lm_rows:
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
        AND dl.entity_id > 100000
    WHERE
      nlc.entity_type = 'MANIFEST'
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
                    print("Updating SC-LM configurations...")
                    cursor.execute("""
                        UPDATE next_location_configs
                        SET is_manual=1, audit_log='LMSC_Migration', next_location_id=%s
                        WHERE entity_type='MANIFEST' AND is_active=1
                        AND location_id=%s AND next_location_id=%s
                        AND pincode_id IN (SELECT pincode_id FROM locations WHERE client_location_name = %s)
                    """, (new_sort_id, curr_sort_id, lmdc_id, lmdc))
                    print(Fore.GREEN + "✅ SC-LM configurations updated successfully")
                else:
                    print("No existing SC-LM records found. Skipping SC-LM update.")
                
                print("Checking LM-SC configurations:")
                lm_sc_results = cursor.execute("""
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
        AND dl.entity_id > 100000
    WHERE
      nlc.entity_type = 'MANIFEST'
      AND nlc.is_active = 1
      AND nlc.location_id = %s
      AND nlc.next_location_id = %s
      AND nlc.pincode_id IN (
        SELECT pincode_id FROM locations WHERE alias = %s
      )
    ORDER BY 
      nlc.updated_at DESC
""", (lmdc_id, curr_sort_id, curr_sort))
                lm_sc_rows = cursor.fetchall()
                if lm_sc_rows:
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
        AND dl.entity_id > 100000
    WHERE
      nlc.entity_type = 'MANIFEST'
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
                    print("Updating LM-SC configurations...")
                    cursor.execute("""
                        UPDATE next_location_configs
                        SET is_manual=1, audit_log='LMSC_Migration', next_location_id=%s
                        WHERE entity_type='MANIFEST' AND is_active=1
                        AND location_id=%s AND next_location_id=%s
                        AND pincode_id IN (SELECT pincode_id FROM locations WHERE alias = %s)
                    """, (new_sort_id, lmdc_id, curr_sort_id, curr_sort))
                    print(Fore.GREEN + "✅ LM-SC configurations updated successfully")
                else:
                    print("No existing LM-SC records found. Skipping LM-SC update.")
                    
                # New sub-step: LM -> Old SC remapping to New SC
                print("Checking LM-Old SC configurations:")
                cursor.execute("""
    SELECT
      nlc.id,
      l.alias AS location,
      ln.alias AS next_location_name,
      dl.alias AS dest_loc_name,
      nlc.pincode_id,
      nlc.updated_at,
      nlc.is_manual,
      nlc.audit_log
    FROM
      next_location_configs nlc
      JOIN locations l 
        ON nlc.location_id = l.id
      JOIN locations ln 
        ON nlc.next_location_id = ln.id
      JOIN locations dl 
        ON dl.pincode_id = nlc.pincode_id
        AND dl.entity_type = 'PARTNER'
        AND dl.status = 1
        AND dl.is_valmo_location = 1
    WHERE 
      nlc.entity_type = 'MANIFEST'
      AND nlc.is_active = 1
      AND l.client_location_name = %s
      AND dl.location_ops_type='SC'
      AND ln.alias <> %s
    ORDER BY 
      nlc.updated_at DESC
""", (lmdc, new_sort))
                lm_old_sc_rows = cursor.fetchall()
                if lm_old_sc_rows:
                    print_query_result(cursor, """
    SELECT
      nlc.id,
      l.alias AS location,
      ln.alias AS next_location_name,
      dl.alias AS dest_loc_name,
      nlc.pincode_id,
      nlc.updated_at,
      nlc.is_manual,
      nlc.audit_log
    FROM
      next_location_configs nlc
      JOIN locations l 
        ON nlc.location_id = l.id
      JOIN locations ln 
        ON nlc.next_location_id = ln.id
      JOIN locations dl 
        ON dl.pincode_id = nlc.pincode_id
        AND dl.entity_type = 'PARTNER'
        AND dl.status = 1
        AND dl.is_valmo_location = 1
    WHERE 
      nlc.entity_type = 'MANIFEST'
      AND nlc.is_active = 1
      AND l.client_location_name = %s
      AND dl.location_ops_type='SC'
      AND ln.alias <> %s
    ORDER BY 
      nlc.updated_at DESC
""", (lmdc, new_sort))
                    lm_old_sc_ids = [row['id'] for row in lm_old_sc_rows]
                    placeholders = ",".join(["%s"] * len(lm_old_sc_ids))
                    print(f"Updating {len(lm_old_sc_ids)} LM-Old SC configurations to point to New Sort Centre: {new_sort}")
                    cursor.execute(f"""
                        UPDATE next_location_configs
                        SET is_manual=1, audit_log='LMSC_Migration', next_location_id=%s
                        WHERE id IN ({placeholders})
                    """, (new_sort_id, *lm_old_sc_ids))
                    print(Fore.GREEN + "✅ LM-Old SC configurations updated successfully")
                else:
                    print("No LM-Old SC records found.")
                
                conn.commit()
                #print("Updated next_location_configs.")

                # Step 5: Insert New Location Configs
                print(Fore.GREEN + "\n➕ Step 5: Creating New Location Configurations...")
                # Inserts: Current Sort Centre -> New Sort Centre and New Sort Centre -> Current Sort Centre
                pincode_new_sort = get_pincode_id_by_location_id(cursor, new_sort_id)
                pincode_curr_sort = get_pincode_id_by_location_id(cursor, curr_sort_id)

                # Check if Current Sort Centre -> New Sort Centre exists
                cursor.execute("""
                    SELECT id FROM next_location_configs
                    WHERE entity_type='MANIFEST' AND is_active=1
                    AND location_id=%s AND next_location_id=%s AND pincode_id=%s
                """, (curr_sort_id, new_sort_id, pincode_new_sort))
                curr_to_new_exists = cursor.fetchone()
                
                if curr_to_new_exists:
                    print("Existing NLC (Current Sort Centre -> New Sort Centre) found, skipping insert:")
                    print_query_result(cursor, """
                        SELECT
                          nlc.id,
                          l.alias AS location,
                          ln.alias AS next_location_name,
                          dl.alias AS dest_loc_name,
                          nlc.pincode_id,
                          nlc.updated_at,
                          nlc.is_manual
                        FROM next_location_configs nlc
                        JOIN locations l ON nlc.location_id = l.id 
                        JOIN locations ln ON nlc.next_location_id = ln.id
                        JOIN locations dl ON dl.pincode_id = nlc.pincode_id
                          AND dl.entity_type = 'PARTNER'
                          AND dl.status = 1 
                          AND dl.entity_id > 100000
                        WHERE nlc.entity_type='MANIFEST' AND nlc.is_active=1
                        AND nlc.location_id=%s AND nlc.next_location_id=%s AND nlc.pincode_id=%s
                    """, (curr_sort_id, new_sort_id, pincode_new_sort))
                else:
                    print("Creating NLC (Current Sort Centre -> New Sort Centre):")
                    print(f"INSERT INTO next_location_configs (location_id,next_location_id,pincode_id,entity_type,is_active,is_manual,audit_log) VALUES ({curr_sort_id},{new_sort_id},{pincode_new_sort},'MANIFEST',1,1,'LMSC_Migration');")
                    ensure_next_location_config(cursor, curr_sort_id, new_sort_id, pincode_new_sort)
                    print(Fore.GREEN + "✅ Current Sort Centre -> New Sort Centre configuration created")

                # Check if New Sort Centre -> Current Sort Centre exists
                cursor.execute("""
                    SELECT id FROM next_location_configs
                    WHERE entity_type='MANIFEST' AND is_active=1
                    AND location_id=%s AND next_location_id=%s AND pincode_id=%s
                """, (new_sort_id, curr_sort_id, pincode_curr_sort))
                new_to_curr_exists = cursor.fetchone()
                
                if new_to_curr_exists:
                    print("Existing NLC (New Sort Centre -> Current Sort Centre) found, skipping insert:")
                    print_query_result(cursor, """
                        SELECT
                          nlc.id,
                          l.alias AS location,
                          ln.alias AS next_location_name,
                          dl.alias AS dest_loc_name,
                          nlc.pincode_id,
                          nlc.updated_at,
                          nlc.is_manual                        FROM next_location_configs nlc
                        JOIN locations l ON nlc.location_id = l.id 
                        JOIN locations ln ON nlc.next_location_id = ln.id
                        JOIN locations dl ON dl.pincode_id = nlc.pincode_id
                          AND dl.entity_type = 'PARTNER'
                          AND dl.status = 1 
                          AND dl.entity_id > 100000
                        WHERE nlc.entity_type='MANIFEST' AND nlc.is_active=1
                        AND nlc.location_id=%s AND nlc.next_location_id=%s AND nlc.pincode_id=%s
                    """, (new_sort_id, curr_sort_id, pincode_curr_sort))
                else:
                    print("Creating NLC (New Sort Centre -> Current Sort Centre):")
                    print(f"INSERT INTO next_location_configs (location_id,next_location_id,pincode_id,entity_type,is_active,is_manual,audit_log) VALUES ({new_sort_id},{curr_sort_id},{pincode_curr_sort},'MANIFEST',1,1,'LMSC_Migration');")
                    ensure_next_location_config(cursor, new_sort_id, curr_sort_id, pincode_curr_sort)
                    print(Fore.GREEN + "✅ New Sort Centre -> Current Sort Centre configuration created")

                conn.commit()

                # Step 6: Create Route
                print(Fore.GREEN + "\n🛣️  Step 6: Creating Route...")
                route_name = f"{new_code} to {new_sort}"
                run_curl_route_creation(lmdc_id, new_sort_id, route_name, token, tokenId)
                    # Commit after all steps succeed
                #conn.commit()
                print(Fore.GREEN + f"✅ Migration completed successfully for {lmdc}")
                remarks.append("Migration completed successfully")
                row['Remarks'] = "; ".join(remarks)
                processed_rows.append(row)

            except Exception as e:
                #conn.rollback()
                print(f"Error processing row{i}: {e}")
                print(Fore.RED + f"Error processing row {i}: {e}")
                remarks.append(f"Error: {str(e)}")
                row['Remarks'] = "; ".join(remarks)
                processed_rows.append(row)

    # Write results to output file
    output_file = "lmscm_output.csv"
    with open(output_file, 'w', newline='') as csvfile:
        if processed_rows:
            fieldnames = list(processed_rows[0].keys())
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(processed_rows)
            print(Fore.GREEN + "\nScript completed. Processed all rows.")
            print(f"Results written to: {output_file}\n")
            print(Fore.CYAN + "SUMMARY:")
            print(Fore.GREEN + f"✅ Total rows processed: {len(processed_rows)}")
            print(Fore.RED + f"❌ Rows skipped due to validation: {skipped_validation}")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    process_csv("lmscm.csv")

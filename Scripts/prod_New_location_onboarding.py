import csv
import requests
import json
import time
import pymysql

# Function to execute SQL queries and fetch results
def execute_query(query, cursor):
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Function for login API
def login_api():
    login_payload = {
        "username": "vineeth.lsn",
        "password": "12345"
    }
    headers = {
        'Content-Type': 'application/json',
        'deviceId': '123123123123'
    }
    response = requests.post('https://log10-api.loadshare.net/v1/login', headers=headers, json=login_payload)
    if response.status_code == 200:
        login_response = response.json()
        token = login_response['response']['token']['accessToken']
        tokenId = login_response['response']['token']['tokenId']
    

        return token, tokenId
    
    else:
        print("Login failed. Please check your credentials.")
        return None, None

# Function for partner location API

# Function for partner location API
def partner_location_api(token, tokenId, partner_name, contact_number, branch_admin_name, email_id, dccode, client_location_name, dc_address, loc_zipcode, delivery_pincodes, city_id, sc, is_loadshare_partner):
    headers = {
        'token': token,
        'tokenId': tokenId,
        'Content-Type': 'application/json'
    }
    # Convert delivery_pincodes to a list of integers
    delivery_pincodes_list = [int(pincode.strip()) for pincode in delivery_pincodes.split(',')]

    payload = {
        "partner": {
            "name": partner_name,
            "contactNumber": int(contact_number),  # Convert to integer
            "isLoadsharePartner": is_loadshare_partner.lower() == 'true'  # Convert to boolean
        },
        "user": {
            "name": branch_admin_name,
            "contactNumber": int(contact_number),  # Convert to integer
            "emailId": email_id  # Correct email field name
        },
        "location": [
            {
                "name": dccode,
                "clientLocationName": client_location_name,
                "addressText": dc_address,
                "locPincode": int(loc_zipcode),  # Convert to integer
                "staffPayCityId": int(city_id),  # Convert to integer
                "locationOnboardingOpsType": "LM",
                "isValmoLocation": True,
                "deliveryPincodes": delivery_pincodes_list,
                "rvpCustomer":[
                    {"pickupPincodes":delivery_pincodes_list,"id":10823,"isSelfServed":False}
                               
                ]  # Use the converted list

            }
        ]
    }
    #print("Request Payload:", json.dumps(payload, indent=2))  # Print request payload for debugging

    response = requests.post('https://log10-api.loadshare.net/b2c/v1/entity-onboarding/partner-location', headers=headers, json=payload)
    #print("insider function",response.json())
    #print("insider function",response.status_code)
    return response
  
def metadata_insert(dccode, sc, cursor, conn):
    # Check if location exists for dccode and sc
    location_query = f"SELECT id FROM locations WHERE alias = '{dccode}' OR alias = '{sc}'"
    location_result = execute_query(location_query, cursor)
    #print(location_query)
    #print(location_result)
    
    if location_result and len(location_result) == 2:  # Both dccode and sc exist
        # Construct alias values
        location_alias = f"{sc}.LMSC"
        next_location_alias = dccode
        
        # Check if the metadata already exists
        check_query = f"SELECT COUNT(*) FROM loadshare.network_metadata WHERE is_active=1 and location_alias = '{location_alias}' AND next_location_alias = '{next_location_alias}'"
        metadata_exists = execute_query(check_query, cursor)
        
        # Extract the count from the first element of the list
        count = metadata_exists[0]['COUNT(*)']
        
        # Insert metadata if it doesn't exist
        if count == 0:
            insert_query = f"INSERT INTO loadshare.network_metadata (location_alias, next_location_alias, is_active) VALUES('{location_alias}', '{next_location_alias}', 1)"
            with conn.cursor() as cursor:
                cursor.execute(insert_query)
                print(f"network metadata inserted: {location_alias} => {next_location_alias}")
            conn.commit()
        else:
            print("network metadata skipped")
    else:
        print(f"network metadata skipped: Location not found for dccode: {dccode} or sc: {sc}")


# Function to insert partner-to-partner mapping into the database
def p2p_mapping(dccode, sc, cursor, conn):
    # Check if location exists for dccode and sc
    location_query = f"SELECT id FROM locations WHERE alias = '{dccode}' OR alias = '{sc}'"
    location_result = execute_query(location_query, cursor)
    #print(location_query)
    #print(location_result)
    
    if location_result and len(location_result) == 2:  # Both dccode and sc exist
        # Get source_partner_id and link_partner_id
        source_query = f"SELECT entity_id FROM locations WHERE alias = '{dccode}'"
        link_query = f"SELECT entity_id FROM locations WHERE alias = '{sc}'"
        source_result = execute_query(source_query, cursor)
        link_result = execute_query(link_query, cursor)
        
        if source_result and link_result:  # Check if both queries returned results
            # Extract source_partner_id from source_result
            source_partner_ids = [row['entity_id'] for row in source_result]
            
            # Extract link_partner_id from link_result
            link_partner_ids = [row['entity_id'] for row in link_result]
            
            # Iterate over source_partner_ids and link_partner_ids
            for source_partner_id in source_partner_ids:
                for link_partner_id in link_partner_ids:
                    # Check if the mapping already exists (source_partner_id to link_partner_id)
                    check_query1 = f"SELECT COUNT(*) FROM partner_to_partner_mapping WHERE is_active=1 and source_partner_id = {source_partner_id} AND link_partner_id = {link_partner_id}"
                    mapping_exists1 = execute_query(check_query1, cursor)
                    
                    # Check if the mapping already exists (link_partner_id to source_partner_id)
                    check_query2 = f"SELECT COUNT(*) FROM partner_to_partner_mapping WHERE is_active=1 and source_partner_id = {link_partner_id} AND link_partner_id = {source_partner_id}"
                    mapping_exists2 = execute_query(check_query2, cursor)
                    
                    # Extract count value from each mapping_exists dictionary
                    count1 = mapping_exists1[0]['COUNT(*)']
                    count2 = mapping_exists2[0]['COUNT(*)']
                    
                    # Insert only if neither mapping exists
                    if count1 == 0 and count2 == 0:
                        # Neither mapping exists, proceed with insertion
                        insert_query1 = f"INSERT INTO partner_to_partner_mapping (source_partner_id, link_partner_id, is_active) VALUES({source_partner_id}, {link_partner_id}, 1)"
                        insert_query2 = f"INSERT INTO partner_to_partner_mapping (source_partner_id, link_partner_id, is_active) VALUES({link_partner_id}, {source_partner_id}, 1)"
                        with conn.cursor() as cursor:
                            cursor.execute(insert_query1)
                            cursor.execute(insert_query2)
                            print(f"p2p inserted: {source_partner_id}=> {link_partner_id} and {link_partner_id} => {source_partner_id} ")
                        conn.commit()
                    elif count1 == 0:
                        # Only mapping from link_partner_id to source_partner_id exists, insert mapping from source_partner_id to link_partner_id
                        insert_query1 = f"INSERT INTO partner_to_partner_mapping (source_partner_id, link_partner_id, is_active) VALUES({source_partner_id}, {link_partner_id}, 1)"
                        with conn.cursor() as cursor:
                            cursor.execute(insert_query1)
                            print(f"p2p inserted: {source_partner_id}=> {source_partner_id}")
                        conn.commit()
                    elif count2 == 0:
                        # Only mapping from source_partner_id to link_partner_id exists, insert mapping from link_partner_id to source_partner_id
                        insert_query2 = f"INSERT INTO partner_to_partner_mapping (source_partner_id, link_partner_id, is_active) VALUES({link_partner_id}, {source_partner_id}, 1)"
                        with conn.cursor() as cursor:
                            cursor.execute(insert_query2)
                            print(f"p2p inserted: {link_partner_id}=> {source_partner_id}")
                        conn.commit()
                    else:
                        print("p2p insertion skipped")
        else:
            print(f"No matching record found in the database for source: {dccode} or destination: {sc}")
    else:
        print(f"p2p insertion skipped:Location not found for dccode: {dccode} or sc: {sc}")

# Function for route creation
def route_creation(dccode, sc, cursor, conn):
    # Check if location exists for dccode and sc
    location_query = f"SELECT id FROM locations WHERE alias = '{dccode}' OR alias = '{sc}'"
    location_result = execute_query(location_query, cursor)
    
    if location_result and len(location_result) == 2:  # Both dccode and sc exist
        source_query = f"SELECT entity_id AS sourcePartnerId, id AS sourceLocationId FROM locations WHERE alias = '{dccode}'"
        destination_query = f"SELECT id AS destinationLocationId FROM locations WHERE alias = '{sc}'"

        source_result = execute_query(source_query, cursor)
        destination_result = execute_query(destination_query, cursor)

        if source_result and destination_result:  # Check if both source and destination queries returned results
            source_partner_id = source_result[0]['sourcePartnerId']  # Access the dictionary within the list
            source_location_id = source_result[0]['sourceLocationId']  # Access the dictionary within the list
            destination_location_id = destination_result[0]['destinationLocationId']  # Access the dictionary within the list
            route_name = f"{dccode} to {sc}"

            # Payload for the login request
            login_payload = {
                "username": "vineeth.lsn",
                "password": "12345"
            }

            # Login API endpoint
            login_url = 'https://log10-api.loadshare.net/v1/login'

            # Perform login request
            headers = {
                'Content-Type': 'application/json',
                'deviceId': '123123123123'
            }
            response = requests.post(login_url, headers=headers, json=login_payload)

            # Check if login was successful
            if response.status_code == 200:
                try:
                    # Extract access token and token ID from response
                    login_response = response.json()
                    token = login_response['response']['token']['accessToken']
                    tokenId = login_response['response']['token']['tokenId']

                    # Hardcoded partner_id and route_creation_url
                    route_creation_url = f'https://log10-api.loadshare.net/b2b/v1/partners/268/routes'

                    # Construct payload for request
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

                    # Perform request with token and tokenId
                    headers = {
                        'token': token,
                        'tokenId': tokenId,
                        'Content-Type': 'application/json'
                    }
                    response = requests.post(route_creation_url, headers=headers, json=payload)
                    
                    # Check if response is valid JSON
                    try:
                        response_json = response.json()
                    except json.JSONDecodeError:
                        print("Failed to decode JSON response.")
                        print("Response content:", response.content)
                        return

                    status_code = response_json['status']['code']
                    message = response_json['status']['message']

                    print(f"Route: {route_name}  Response code: {status_code}, Message: {message}")

                except KeyError:
                    print("Failed to retrieve access token and token ID from login response.")
            else:
                print("Login failed. Please check your credentials.")
        else:
            print(f"No matching record found in the database for source: {dccode} or destination: {sc}")
    else:
        print(f"route creation skipped:Location not found for dccode: {dccode} or sc: {sc}")

# Function to update enableWrongFacilityShipmentScan with location_id
def update_wrong_facility_scan(dccode, conn):
    location_id_query = f"SELECT id AS location_id FROM locations WHERE alias = '{dccode}'"
    location_id_result = execute_query(location_id_query, conn.cursor())
    
    if location_id_result:  # Check if the query result is not empty
        location_id = location_id_result[0]['location_id']  # Access the first dictionary in the list
        is_present_query = f"SELECT JSON_CONTAINS(config->'$.enableWrongFacilityShipmentScan', '{location_id}') AS is_present FROM application_config WHERE id = 1"
        is_present_result = execute_query(is_present_query, conn.cursor())
        
        if not is_present_result[0]['is_present']:  # Access the first dictionary in the list
            update_query = """
            UPDATE application_config
            SET config = JSON_SET(
                            config, 
                            '$.enableWrongFacilityShipmentScan',
                            JSON_ARRAY_INSERT(
                                COALESCE(JSON_EXTRACT(config, '$.enableWrongFacilityShipmentScan'), JSON_ARRAY()),
                                '$[0]',
                                %s
                            )
                        )
            WHERE id = 1;
            """
            with conn.cursor() as cursor:
                cursor.execute(update_query, (location_id,))
                conn.commit()
            print(f"WrongFacility updated for location: {dccode} => {location_id}")
        else:
            print(f"Location: {dccode} => {location_id} already present in enableWrongFacilityShipmentScan")
    else:
        print(f"wrong_facility config skipped:No location ID found for dccode: {dccode}")
def update_call_masking_exclude_location(dccode, conn):
    location_id_query = f"SELECT id AS location_id FROM locations WHERE alias = '{dccode}'"
    location_id_result = execute_query(location_id_query, conn.cursor())
    
    if location_id_result:  # Check if the query result is not empty
        location_id = location_id_result[0]['location_id']  # Access the first dictionary in the list
        is_present_query = f"SELECT JSON_CONTAINS(config->'$.call_masking_exclude_location', '{location_id}') AS is_present FROM application_config WHERE id = 1"
        is_present_result = execute_query(is_present_query, conn.cursor())
        
        if not is_present_result[0]['is_present']:  # Access the first dictionary in the list
            update_query = """
            UPDATE application_config
            SET config = JSON_SET(
                            config, 
                            '$.call_masking_exclude_location',
                            JSON_ARRAY_INSERT(
                                COALESCE(JSON_EXTRACT(config, '$.call_masking_exclude_location'), JSON_ARRAY()),
                                '$[0]',
                                %s
                            )
                        )
            WHERE id = 1;
            """
            with conn.cursor() as cursor:
                cursor.execute(update_query, (location_id,))
                conn.commit()
            print(f"Call masking exclusion updated for location: {dccode} => {location_id}")
        else:
            print(f"Location: {dccode} => {location_id} already present in call_masking_exclude_location")
    else:
        print(f"Call masking exclusion config skipped: No location ID found for dccode: {dccode}")


# Main function to read input file and process data
def process_input_and_configure_system():
    # MySQL database configuration
    mysql_host = 'log10-replica-replica.cco3osxqlq4g.ap-south-1.rds.amazonaws.com'
    mysql_port = 3306  # Assuming default MySQL port
    mysql_user = 'log10_scripts'
    mysql_password = 'D2lx7Wz0Wm9ISAY-Vp1gxKDTzLRRl5k1m'
    mysql_db = 'loadshare'

    # Read data from CSV file and process
    with open('new_partner_input_file.csv', mode='r') as file:
        csv_reader = csv.DictReader(file)
        total_iterations = sum(1 for _ in csv_reader)  # Count total iterations
        file.seek(0)  # Reset file pointer to the beginning
        next(csv_reader)  # Skip the header row
        for i, row in enumerate(csv_reader, start=1):
            try:
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
                print(f"Iteration: {i}/{total_iterations}")
                print(f"Location: {dccode}")
                print("--------")

                # Get token and tokenId
                token, tokenId = login_api()
                # Call partner location API
                response = partner_location_api(token, tokenId, partner_name, contact_number, branch_admin_name, email_id, dccode, client_location_name, dc_address, loc_zipcode, delivery_pincodes, city_id, sc, is_loadshare_partner)
                if response.status_code == 200:
                    try:
                        response_json = response.json()
                        status_code = response_json['status']['code']
                        message = response_json['status']['message']
                        if status_code == 202:
                            if 'response' in response_json and 'entityDetails' in response_json['response']:
                                entity_details = response_json['response']['entityDetails']
                                print("Location Onboarded Successfully:")
                                print(f"Partner Name: {entity_details.get('partnerName')}")
                                print(f"Location ID: {entity_details.get('locationId')}")
                                print(f"Location Name: {entity_details.get('locationName')}")
                            else:
                                print("No partner details found in the response.")
                            # Insert metadata                          
                            conn = pymysql.connect(
                                        host=mysql_host,
                                        port=mysql_port,
                                        user=mysql_user,
                                        password=mysql_password,
                                        db=mysql_db,
                                        charset='utf8mb4',
                                        cursorclass=pymysql.cursors.DictCursor
                            )
                            metadata_insert(dccode, sc, conn.cursor(), conn)
                            conn.close()
                            # Insert partner-to-partner mapping
                            conn = pymysql.connect(
                                        host=mysql_host,
                                        port=mysql_port,
                                        user=mysql_user,
                                        password=mysql_password,
                                        db=mysql_db,
                                        charset='utf8mb4',
                                        cursorclass=pymysql.cursors.DictCursor
                            )
                            p2p_mapping(dccode, sc, conn.cursor(), conn)
                            conn.close()
                            # Update configuration
                            conn = pymysql.connect(
                                        host=mysql_host,
                                        port=mysql_port,
                                        user=mysql_user,
                                        password=mysql_password,
                                        db=mysql_db,
                                        charset='utf8mb4',
                                        cursorclass=pymysql.cursors.DictCursor
                            )
                            update_wrong_facility_scan(dccode, conn)
                            conn.close()
                            # Create route
                            conn = pymysql.connect(
                                        host=mysql_host,
                                        port=mysql_port,
                                        user=mysql_user,
                                        password=mysql_password,
                                        db=mysql_db,
                                        charset='utf8mb4',
                                        cursorclass=pymysql.cursors.DictCursor
                            )
                            update_call_masking_exclude_location(dccode, conn)
                            conn.close()
                            # Create route
                            conn = pymysql.connect(
                                        host=mysql_host,
                                        port=mysql_port,
                                        user=mysql_user,
                                        password=mysql_password,
                                        db=mysql_db,
                                        charset='utf8mb4',
                                        cursorclass=pymysql.cursors.DictCursor
                            )                  
                            route_creation(dccode, sc, conn.cursor(), conn)
                            conn.close()
                            print()
                        else:
                            print(f"Location onboarding failed. Status Code: {status_code}, Message: {message}")
                            print()
                    except json.JSONDecodeError:
                        print("Failed to decode JSON response.")
                        print("Response content:", response.content)
                else:
                    print(f"location onboarding API failed with status code: {response.status_code}")
                    print()
            except KeyError:
                print("Column 'dccode' or 'sc' not found in the input file.")
            except Exception as e:
                print(f"An error occurred: {str(e)}")
                continue  # Continue to the next iteration even if an error occurs

    # Close MySQL connection
    

# Call the main function
process_input_and_configure_system()

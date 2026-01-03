import csv
import requests
import json
import pymysql

# Function to execute SQL queries and fetch results
# Function to execute SQL queries and fetch results
def execute_query(query, cursor):
    #print("Executing query:", query)  # Print the SQL query
    cursor.execute(query)
    result = cursor.fetchall()  # Fetch all records
    #print("Query result:", result)  # Print the result
    return result

# Function to insert partner-to-partner mapping into the database
def p2p_mapping(dccode, sc, cursor, conn):
    # Check if location exists for dccode and sc
    location_query = f"SELECT id FROM locations WHERE alias = '{dccode}' OR alias = '{sc}' and entity_type='partner' and status=1 "
    location_result = execute_query(location_query, cursor)
    print(location_query)
    print(location_result)
    
    if location_result and len(location_result) == 2:  # Both dccode and sc exist
        # Get source_partner_id and link_partner_id
        source_query = f"SELECT entity_id FROM locations WHERE alias = '{dccode}' and entity_type='partner' and status=1"
        link_query = f"SELECT entity_id FROM locations WHERE alias = '{sc}' and entity_type='partner' and status=1 "
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
    location_query = f"SELECT id FROM locations WHERE alias = '{dccode}' OR alias = '{sc}' and entity_type='partner' and status=1 "
    location_result = execute_query(location_query, cursor)
    
    if location_result and len(location_result) == 2:  # Both dccode and sc exist
        source_query = f"SELECT entity_id AS sourcePartnerId, id AS sourceLocationId FROM locations WHERE alias = '{dccode}' "
        destination_query = f"SELECT id AS destinationLocationId FROM locations WHERE alias = '{sc}' and entity_type='partner' and status=1 "

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

# Main function to read input file and process data
def process_input_and_configure_system():
    # SSH tunnel configuration
    mysql_host = 'log10-replica-replica.cco3osxqlq4g.ap-south-1.rds.amazonaws.com'
    mysql_port = 3306  # Assuming default MySQL port
    mysql_user = 'log10_scripts'
    mysql_password = 'D2lx7Wz0Wm9ISAY-Vp1gxKDTzLRRl5k1m'
    mysql_db = 'loadshare'

    # Create SSH tunnel
        # Connect to MySQL via the SSH tunnel
    conn = pymysql.connect(
                host=mysql_host,
                port=mysql_port,
                user=mysql_user,
                password=mysql_password,
                db=mysql_db,
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor
    )
                            
    with open('input.csv', mode='r') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                try:
                    dccode = row.get('dccode', '').strip()
                    sc = row.get('sc', '').strip()
                    print(dccode)
                    print("-------")
                    # Insert partner-to-partner mapping
                    p2p_mapping(dccode, sc, conn.cursor(), conn)

                    # Create route
                    route_creation(dccode, sc, conn.cursor(), conn)
                    print()

                except KeyError:
                    print("Column 'dccode' or 'sc' not found in the input file.")

        # Close MySQL connection
    conn.close()

# Call the main function
process_input_and_configure_system()

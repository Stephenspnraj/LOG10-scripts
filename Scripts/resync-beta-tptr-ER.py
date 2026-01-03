import requests
import json
import time
import pymysql
import csv
from datetime import datetime

# MySQL database connection details
mysql_host = 'log10-replica-replica.cco3osxqlq4g.ap-south-1.rds.amazonaws.com'
mysql_port = 3306
mysql_user = 'log10_scripts'
mysql_password = 'D2lx7Wz0Wm9ISAY-Vp1gxKDTzLRRl5k1m'
mysql_db = 'loadshare'

def get_trip_codes_from_csv(csv_path):
    trip_codes = []
    with open(csv_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if 'trip_code' in row:
                trip_codes.append(row['trip_code'])
    return trip_codes

def process_beta_tptr(trip_code):

    if not trip_code:
        print("No trip codes provided.")
        return

    conn = pymysql.connect(
        host=mysql_host,
        port=mysql_port,
        user=mysql_user,
        password=mysql_password,
        db=mysql_db,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )
    cursor = conn.cursor()

    # Prepare trip_code string for SQL IN clause
    trip_code_str = ",".join(["'%s'" % code for code in trip_code])
    tptr_select_query = f"""
    SELECT *
    FROM third_party_trips_request tptr
    WHERE tptr.trip_reference_number IN ({trip_code_str})
      AND tptr.customer_unique_identifier = 'SHIPSY'
      AND tptr.request_type = 'TRIPS_OUTGOING'
    GROUP BY tptr.sync_entity_id;
    """

    cursor.execute(tptr_select_query)
    tptr_requests = cursor.fetchall()

    if tptr_requests:
        for request_row in tptr_requests:
            request_json = json.loads(request_row['request'])
            request_json = json.loads(request_json['request'])

            # print(f"Processing trip code: {request_row['trip_reference_number']}")
            # print(f"Request JSON: {request_json}")

            
            if 'trip_origin_hub' in request_json:
                toh = request_json['trip_origin_hub']
                if isinstance(toh, str) and len(toh) == 4 and toh[3] == 'L':
                     request_json['trip_origin_hub'] = toh[:3]

            if 'connection_origin' in request_json:
                coh = request_json['connection_origin']
                if isinstance(coh, str) and len(coh) == 4 and coh[3] == 'L':
                    request_json['connection_origin'] = coh[:3]
            
            if 'current_hub' in request_json:
                coh = request_json['current_hub']
                if isinstance(coh, str) and len(coh) == 4 and coh[3] == 'L':
                    request_json['current_hub'] = coh[:3]

            # print(f"Request JSON: {request_json}")
            # Sync updated request_json to external URL
            url = "https://webhook.shipsy.io/api/webhook/Ixktvtn-uBB54xH0P-PORRwPr0FiSlMq9KGb4miY9sqlueXZa_ad0cq0Q1qrmi4i5eCfYyqjd7EtiyXDL9Tf8A=="
            headers = {
                "MERCHANT": "loadshare_trip",
                "AUTHORIZATION": "e0c4069e-a25b-4fae-9433-534a8e2d6940",
                "Content-Type": "application/json"
            }

            start_time = time.time()
            #print(f"Request JSON: {request_json}")
            response = requests.post(url, headers=headers, data=json.dumps(request_json))
            end_time = time.time()
            response_text = response.text
            time_taken_in_secs = int(end_time - start_time)

            formatted_request = json.dumps({
                "request": json.dumps(request_json),
                "header": json.dumps(headers)
            })

            # Insert new row with modified request and response
            insert_query = """
            INSERT INTO third_party_trips_request (
                customer_id,
                trip_reference_number,
                customer_unique_identifier,
                request,
                request_type,
                response,
                status,
                created_at,
                updated_at,
                time_taken_in_secs,
                url,
                sync_entity_id,
                location_id,
                sync_entity_type
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            cursor.execute(
                insert_query,
                (
                    request_row['customer_id'],
                    request_row['trip_reference_number'],
                    request_row['customer_unique_identifier'],
                    formatted_request,
                    request_row['request_type'],
                    response_text,
                    #response.status_code,
                    'SUCCESS' if response.status_code == 200 else 'FAILURE',
                    now,
                    now,
                    time_taken_in_secs,
                    url,
                    request_row['sync_entity_id'],
                    request_row['location_id'],
                    request_row['sync_entity_type']
                )
            )
            print(f"Trip code: {request_row['trip_reference_number']}, Response status: {response.status_code}")
            conn.commit()

    cursor.close()
    conn.close()

# Example usage
csv_path = 'trip_codes.csv'  # Update with your CSV file path
trip_codes = get_trip_codes_from_csv(csv_path)
process_beta_tptr(trip_codes)

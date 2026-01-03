import pymysql
import json
import requests
import sys

# Database configuration
# read-only
# MYSQL_HOST = 'log-10-replica-single.cco3osxqlq4g.ap-south-1.rds.amazonaws.com'
# MYSQL_PORT = 3306  
# MYSQL_USER = 'meesho'
# MYSQL_PASSWORD = 'dYDxdwV*qf6rDcXiWFkCCcVH'
# MYSQL_DB = 'loadshare'

# write
MYSQL_HOST = 'log10-replica-replica.cco3osxqlq4g.ap-south-1.rds.amazonaws.com'
MYSQL_PORT = 3306  
MYSQL_USER = 'log10_scripts'
MYSQL_PASSWORD = 'D2lx7Wz0Wm9ISAY-Vp1gxKDTzLRRl5k1m'
MYSQL_DB = 'loadshare'

# Function to get a database connection
def get_db_connection():
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        db=MYSQL_DB,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

def shipsy_retry(event_ids: str) -> None:
    if not event_ids:
        print("Error: No event IDs provided.")
        sys.exit(1)  # Exit if no event_ids are passed

    print(f"Event IDs: {event_ids}")

    # Connect to the database
    db = get_db_connection()

    try:
        # API URL for retrying SHIPSY events
        API_URL = "https://webhook.shipsy.io/api/webhook/Ixktvtn-uBB54xH0P-PORRwPr0FiSlMq9KGb4miY9sqlueXZa_ad0cq0Q1qrmi4i5eCfYyqjd7EtiyXDL9Tf8A=="
        
        # Define empty headers as required by the base script
        headers = {
            "x-api-key": ""  # Empty header
        }
        
        # Fetch event details based on the provided event_ids
        event_ids_str = ','.join(f"'{event_id.strip()}'" for event_id in event_ids.split(',') if event_id.strip())
        
        with db.cursor() as cursor:
            cursor.execute(f"SELECT * FROM third_party_trips_request WHERE request_type = 'TRIPS_OUTGOING' AND id IN ({event_ids_str})")
            trip_events = cursor.fetchall()

        if not trip_events:
            print("Error: No valid events found for the provided event_ids.")
            sys.exit(1)

        for event in trip_events:
            # Extract request payload from the event
            request_payload = json.loads(json.loads(event['request'])['request'])
            print(f"Sending request to {API_URL} with payload: {request_payload} and headers: {headers}")

            # Send the POST request to SHIPSY API with headers
            response = requests.post(API_URL, json=request_payload, headers=headers)
            print(response)

            if response.status_code == 200:
                # Update the row with status 'PROCESSED' for successfully retried events
                with db.cursor() as cursor:
                    cursor.execute(
                        "UPDATE third_party_trips_request SET status = %s WHERE id = %s",
                        ('SUCCESS', event['id'])
                    )
                    db.commit()
                print(f"Successfully retried and updated event with event_id: {event['id']}")
            else:
                print(f"Failed to retry event_id: {event['id']}, HTTP Status Code: {response.status_code}")

    finally:
        db.close()

if __name__ == "__main__":
    # Example usage: Pass event_ids as a command line argument
    if len(sys.argv) > 1:
        shipsy_retry(sys.argv[1])
    else:
        print("Error: Please provide event IDs as input.")
        sys.exit(1)

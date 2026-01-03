import pymysql
import time
import boto3
import json
import sys

TRIPS_QUEUE_URL = 'https://sqs.ap-south-1.amazonaws.com/622676093614/meesho-prod_third_party_trip_events_queue.fifo'

MYSQL_HOST = 'log-10-replica-single.cco3osxqlq4g.ap-south-1.rds.amazonaws.com'
MYSQL_PORT = 3306  
MYSQL_USER = 'meesho'
MYSQL_PASSWORD = 'dYDxdwV*qf6rDcXiWFkCCcVH'
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

# The input 'event_ids' is removed from the function signature
def shipsy_retry() -> None:
    # No input validation needed here as the IDs are fetched internally

    sqs = boto3.client('sqs', region_name="ap-south-1")

    # Connect to the database
    db = get_db_connection()

    try:
        # --- MODIFIED: Automatically fetch events based on your query logic ---
        with db.cursor() as cursor:
            # Modified query to fetch id and trip_reference_number for PENDING events
            cursor.execute("""
                SELECT id, trip_reference_number 
                FROM third_party_trips_request 
                WHERE customer_unique_identifier = 'SHIPSY'
                AND request_type = 'SHIPSY_TRIPS_INCOMING' 
                AND status = 'PENDING'
                AND created_at < NOW() - INTERVAL 3 HOUR
                AND created_at > NOW() - INTERVAL 6 HOUR;
            """)
            trip_events = cursor.fetchall()

        if not trip_events:
            print("No matching PENDING events found to retry in the last 3-6 hours.")
            sys.exit(0) # Exit successfully if nothing is found (as requested)

        print(f"Enqueuing {len(trip_events)} events to SQS")
        for event in trip_events:
            try:
                message = {
                    'thirdPartyTripsRequestId': event['id'],
                    'start': str(round(time.time()))
                }

                message_attributes = {
                    'eventType': {
                        'StringValue': 'SHIPSY_TRIP_EVENTS',
                        'DataType': 'String'
                    }
                }

                # MessageGroupId requires 'trip_reference_number' which is now selected by the query
                response = sqs.send_message(
                    QueueUrl=TRIPS_QUEUE_URL,
                    MessageBody=json.dumps(message),
                    MessageAttributes=message_attributes,
                    MessageGroupId=event['trip_reference_number']
                )
                print(f"Success: Event ID {event['id']} enqueued successfully. Response: {response}")
            except Exception as e:
                print(f"Error: Failed to enqueue event ID {event['id']}. Error: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    # --- MODIFIED: Call the function without arguments and remove argument checks ---
    shipsy_retry()

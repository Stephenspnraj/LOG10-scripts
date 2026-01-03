import pymysql
import time
import boto3
import json
import sys

TRIPS_QUEUE_URL = 'https://sqs.ap-south-1.amazonaws.com/622676093614/meesho-prod_third_party_trip_events_queue.fifo'

# Database configuration
# MYSQL_HOST = 'log10-staging.cco3osxqlqnaws.com'
# MYSQL_PORT = 3306
# MYSQL_USER = 'log10_staging'
# MYSQL_PASSWORD = 'AhL4go6ekP'
# MYSQL_DB = 'loadshare'
# Prod Read Replica
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

def shipsy_retry(event_ids: str) -> None:
    if not event_ids:
        print("Error: No event IDs provided. Please pass event_ids as input.")
        sys.exit(1)  # Exit script with failure code

    # Convert comma-separated string to list of event_ids
    event_ids_list = [event_id.strip() for event_id in str(event_ids).split(',') if event_id.strip()]
    if not event_ids_list:
        print("Error: Invalid event IDs provided.")
        sys.exit(1)

    sqs = boto3.client('sqs', region_name="ap-south-1")

    # Connect to the database
    db = get_db_connection()

    try:
        # Fetch events based on provided event IDs
        event_ids_str = ','.join(f"'{event_id}'" for event_id in event_ids_list)  # Handle string IDs
        with db.cursor() as cursor:
            cursor.execute(f"SELECT * FROM third_party_trips_request WHERE request_type = 'SHIPSY_TRIPS_INCOMING' AND id IN ({event_ids_str})")
            trip_events = cursor.fetchall()

        if not trip_events:
            print("No matching events found for the provided event IDs.")
            sys.exit(1)  # Exit script with failure code

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
    # Example usage: Pass event_ids as a command line argument
    if len(sys.argv) > 1:
        shipsy_retry(sys.argv[1])
    else:
        print("Error: Please provide event IDs as input.")
        sys.exit(1)

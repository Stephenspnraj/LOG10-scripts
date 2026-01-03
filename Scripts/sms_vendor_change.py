import csv
import pymysql
import requests
import boto3
from botocore.exceptions import ClientError

# =======================================================
mysql_host = 'prod-titan-rds.cco3osxqlq4g.ap-south-1.rds.amazonaws.com'
mysql_port = 3306  
mysql_user = 'prod_titan_app'
mysql_password = '7Xc8ZG0bEpFTkhrrGoRAlhaP5qV9zSkfEagQ'
mysql_db = 'titan'

# AWS Configuration
# =======================================================
AWS_REGION = 'ap-south-1'
TARGET_GROUP_NAMES = ['titan-communication-service-tg-2', 'prod-elb-titan-erp-tg']

# API Configuration
# =======================================================
AUTH_TOKEN = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJMRUFEX0lEIjowLCJVU0VSX0lEIjoxNjE5MjM4MDgzODYyNzgsIlVTRVJfQ09OVEFDVF9OVU1CRVIiOiI5NzkxNzQwODMxIiwiZXhwIjoyNTM3NjkxMTM3LCJVU0VSX09SR19JRCI6Mn0.ifS_L-TQgg1P6gCVJZ-Z3xp-IjQC-s1wlUYo65Q8YzrscfsPYq0I8TEBCOYXRWA2wr2TBg3HOIq12Z0Uh9mjpggyV-vtDmpTblrc9Qjpoo4DvLD1G3a5n6mSVMPpQOnCcdyLH3IoDbHlH2qLGAiwwWQNKUDK71j2Rx2PqSptzLBfEjvo_G6b0zbc2Nr_HY-OzLlAM_9O6uaOmst-zKn6Z4ztW1zkKAuMpbKVfFKtz6ibzMnZX6wp0mkIUYjAU5mvr9nfyiRIAa_k8A88nqf_cuPKtsZMoYcf-eTEbcio7rjk2WCUiLEOWBDFha8RgcOE0I5UDScQCnbK6K6leYhVPg'
HEADERS = {
    'authtoken': AUTH_TOKEN,
    'clientid': ''
}

# Database Connection Handler
# =======================================================
def get_db_connection():
    """Establish MySQL connection with tunnel-ready configuration"""
    try:
        return pymysql.connect(
            host=mysql_host,
            port=mysql_port,
            user=mysql_user,
            password=mysql_password,
            db=mysql_db,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
    except pymysql.MySQLError as e:
        print(f"MySQL Connection Error: {e}")
        raise

# AWS Instance Fetcher
# =======================================================
def get_instances_from_target_group(target_group_name):
    """Fetch instances and ports from AWS target group"""
    elbv2 = boto3.client('elbv2', region_name=AWS_REGION)
    ec2 = boto3.client('ec2', region_name=AWS_REGION)
    
    try:
        # Get target group ARN
        tg_info = elbv2.describe_target_groups(Names=[target_group_name])
        if not tg_info['TargetGroups']:
            print(f"Target group {target_group_name} not found")
            return []
            
        tg_arn = tg_info['TargetGroups'][0]['TargetGroupArn']
        
        # Get target health descriptions
        targets = elbv2.describe_target_health(TargetGroupArn=tg_arn)
        instances = []
        
        for target in targets['TargetHealthDescriptions']:
            instance_id = target['Target']['Id']
            port = target['Target']['Port']
            
            # Get instance public IP
            instances_response = ec2.describe_instances(InstanceIds=[instance_id])
            if instances_response['Reservations']:
                public_ip = instances_response['Reservations'][0]['Instances'][0].get('PublicIpAddress')
                if public_ip:
                    instances.append({'ip': public_ip, 'port': port})
                else:
                    print(f"No public IP found for instance {instance_id}")
                    
        return instances
        
    except ClientError as e:
        print(f"AWS API error: {e}")
        return []

# API Request Executor
# =======================================================
def execute_curl_requests():
    """Execute CURL requests and return True only if all succeed"""
    all_success = True
    for target_group in TARGET_GROUP_NAMES:
        print(f"\nProcessing target group: {target_group}")
        instances = get_instances_from_target_group(target_group)
        
        if not instances:
            print(f"No instances found in target group {target_group}")
            all_success = False
            continue
            
        for instance in instances:
            url = f"http://{instance['ip']}:{instance['port']}/comm-service/cache/invalidate"
            try:
                response = requests.delete(url, headers=HEADERS, timeout=5)
                print(f"Request to {url} - Status Code: {response.status_code}")
                if response.status_code not in [200, 201, 202, 204]:
                    all_success = False
            except Exception as e:
                status_code = getattr(e.response, 'status_code', 'No response')
                print(f"Failed to execute request to {url}: {str(e)} (Status: {status_code})")
                all_success = False
    return all_success
# CSV Processor
# =======================================================
def process_csv(csv_file_path):
    """Main CSV processing logic"""
    try:
        conn = get_db_connection()
        print("Successfully connected to MySQL database")
        
        # Test connection with sample query
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1")
            print("Database connection verified")

    except Exception as e:
        print(f"Failed to initialize database connection: {e}")
        return

    try:
        with open(csv_file_path, 'r') as csvfile:
            csvreader = csv.DictReader(csvfile)
            
            for row in csvreader:
                event_name = row['event_name']
                new_vendor = row['vendor']
                print(f"\nProcessing event: {event_name} with vendor: {new_vendor}")

                with conn.cursor() as cursor:
                    # Check if event exists
                    cursor.execute(
                        "SELECT * FROM titan.communication_event_vendor_mapping WHERE `type` = 'SMS' AND event_name = %s", (event_name,)
                    )
                    event_data = cursor.fetchone()

                    if not event_data:
                        print(f"Event {event_name} not found in database. Skipping.")
                        continue

                    current_vendor = event_data['vendor']
                    whitelisted_vendors = [v.strip() for v in event_data['whitelisted_vendors'].split(',')]

                    # Vendor check logic
                    if new_vendor == current_vendor:
                        print(f"Vendor same as current ({current_vendor}). No action needed.")
                        continue

                    if new_vendor not in whitelisted_vendors:
                        print(f"Vendor {new_vendor} not whitelisted. Allowed: {', '.join(whitelisted_vendors)}")
                        continue

                    # Update vendor
                    try:
                        cursor.execute(
                            "UPDATE titan.communication_event_vendor_mapping SET vendor = %s WHERE `type` = 'SMS' AND event_name = %s", (new_vendor, event_name)
                        )
                            # Execute API calls and check if ALL succeeded
                        api_success = execute_curl_requests()
                        
                        if api_success:
                            conn.commit()
                            print(f"Successfully updated vendor to {new_vendor} and cleared caches")
                        else:
                            conn.rollback()
                            print("Rolling back changes due to API failures")

                    except pymysql.MySQLError as e:
                        print(f"Update failed: {e}")
                        conn.rollback()

    except FileNotFoundError:
        print(f"CSV file not found: {csv_file_path}")
    except Exception as e:
        print(f"Processing error: {e}")
    finally:
        conn.close()
        print("\nMySQL connection closed")

# Main Execution
# =======================================================
if __name__ == "__main__":
    csv_path = "vendor_switch.csv"
    process_csv(csv_path)

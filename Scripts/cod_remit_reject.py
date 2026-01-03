import csv
import requests
import pymysql
from datetime import timedelta, datetime

# Database connection parameters
mysql_host = 'log-10-replica-single.cco3osxqlq4g.ap-south-1.rds.amazonaws.com'
mysql_port = 3306
mysql_user = 'meesho'
mysql_password = 'dYDxdwV*qf6rDcXiWFkCCcVH'
mysql_db = 'loadshare'

# API endpoints
login_url = 'https://log10-api.loadshare.net/v1/login'
fetch_settlement_info_url = 'https://log10-api.loadshare.net/b2b/v1/cod/settlements'
reject_settlement_url = 'https://log10-api.loadshare.net/b2b/v1/cod/approve'

# Step 1: Connect to the database
def get_db_connection():
    return pymysql.connect(
        host=mysql_host,
        port=mysql_port,
        user=mysql_user,
        password=mysql_password,
        db=mysql_db,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

# Step 2: Validate settlement transaction and fetch additional fields
def validate_transaction(transaction_id):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            query_valid = f"""
             SELECT created_at, partner_id, approver_partner_id, approval_status FROM settlement_info WHERE id={transaction_id} AND approval_status='REQUEST_FOR_APPROVAL';
            """
            cursor.execute(query_valid)
            result = cursor.fetchone()
            
            if result:
                if result["approval_status"] == "REJECTED":
                    print(f"Transaction ID {transaction_id} is already rejected.")
                    return None  # Skip processing this transaction
                return result  # Valid case with additional required fields
            else:
                cursor.execute(f"SELECT approval_status FROM settlement_info WHERE id={transaction_id}")
                not_valid_reason = cursor.fetchone()
                print(f"Transaction ID {transaction_id} is not valid. Reason: {not_valid_reason}")
                return None  # Transaction is invalid

# Step 3: Login to get token and tokenId
def login():
    login_payload = {
        "username": "vineeth.lsn",
        "password": "12345"
    }
    headers = {
        "Content-Type": "application/json",
        "deviceId": "123123123123"
    }
    response = requests.post(login_url, headers=headers, json=login_payload)

    if response.status_code == 200:
        try:
            login_response = response.json()
            token = login_response['response']['token']['accessToken']
            tokenId = login_response['response']['token']['tokenId']
            return token, tokenId
        except KeyError:
            print("Login response did not contain expected token data.")
            return None, None
    else:
        print("Login failed with status code:", response.status_code)
        return None, None

# Step 4: Fetch settlement info based on additional fields and time window
def fetch_settlement_info(token, tokenId, created_at, partner_id, approver_partner_id, transaction_id):
    five_minutes = timedelta(minutes=5)
    start_date = int((created_at - five_minutes).timestamp() * 1000)  # Convert to epoch in milliseconds
    end_date = int((created_at + five_minutes).timestamp() * 1000)    # Convert to epoch in milliseconds

    headers = {
        "token": token,
        "tokenId": tokenId,
        "Content-Type": "application/json"
    }
    payload = {
        "dateRange": {"from": start_date, "to": end_date},
        "startDate": start_date,
        "endDate": end_date,
        "approverPartnerId": [approver_partner_id],
        "partnerId": [partner_id],
        "transactionIds": [transaction_id],
        "pageNo": 1,
        "pageSize": 1
    }
    response = requests.post(fetch_settlement_info_url, headers=headers, json=payload)

    if response.ok:
        response_data = response.json()
        settlement_infos = response_data.get("response", {}).get("settlementInfos")
        if settlement_infos:
            print("Settlement info fetch API has data.")
            return settlement_infos[0]  # Return the first matching settlementInfo
        else:
            print(f"No settlementInfo found for transaction ID {transaction_id}.")
            return None
    else:
        print(f"Error fetching settlement info: {response.status_code}")
        return None

# Step 5: Reject settlement
def reject_settlement(token, tokenId, settlement_info):
    settlement_info["approvalStatus"] = "REJECTED"
    
    headers = {
        "token": token,
        "tokenId": tokenId,
        "Content-Type": "application/json"
    }
    payload = {
        "remitter": {},
        "amount": {
            "amount": 0,
            "currency": "INR"
        },
        "payee": {},
        "paymentEntity": {},
        "settlementInfo": settlement_info,
        "remitanceType": "PARTNER_TO_CUSTOMER_REMITTANCE",
        "approvalUser": {
            "id": 49055  # Fixed approval user ID
        }
    }
    
    response = requests.post(reject_settlement_url, headers=headers, json=payload)
    
    try:
        response_data = response.json()
        if response.status_code == 200 and response_data.get("status", {}).get("code") == 202:
            print(f"Settlement with ID {settlement_info['id']} rejected successfully.")
        else:
            internal_code = response_data.get("status", {}).get("code", "Unknown")
            message = response_data.get("status", {}).get("message", "No message provided")
            print(f"Failed to reject settlement with ID {settlement_info['id']}.")
            print(f"Status_code: {internal_code}")
            print(f"Message: {message}")
    except ValueError:
        print(f"Failed to reject settlement with ID {settlement_info['id']}. HTTP Status: {response.status_code}, response body: {response.text}")

# Main process
def process_rejections(csv_file_path):
    token, tokenId = login()
    if not token or not tokenId:
        print("Exiting: Unable to obtain login tokens.")
        return

    with open(csv_file_path, 'r') as file:
        csv_reader = csv.reader(file)
        next(csv_reader)  # Skip header if present

        total_rows = sum(1 for row in csv_reader)
        file.seek(0)
        next(csv_reader)  # Reset and skip header again

        for idx, row in enumerate(csv_reader, start=1):
            transaction_id = int(row[0])
            print(f"\nIteration: {idx}/{total_rows}")
            print(f"Transaction_ID: {transaction_id}\n--------")

            validation_result = validate_transaction(transaction_id)
            
            if not validation_result:
                print(f"Skipping invalid transaction ID {transaction_id}.")
                continue  # Skip further processing for this transaction if invalid

            partner_id = validation_result["partner_id"]
            approver_partner_id = validation_result["approver_partner_id"]
            created_at = validation_result["created_at"]

            settlement_info = fetch_settlement_info(token, tokenId, created_at, partner_id, approver_partner_id, transaction_id)
            if settlement_info:
                reject_settlement(token, tokenId, settlement_info)
            else:
                print(f"No settlementInfo found for transaction ID {transaction_id}.")

# Execute the script by calling the main process
csv_file_path = 'reject.csv'  # Update this path
process_rejections(csv_file_path)

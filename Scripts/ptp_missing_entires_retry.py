import psycopg2
import requests
import json

# Redshift Connection Details
REDSHIFT_HOST = '13.235.224.8'
REDSHIFT_PORT = '5439'
REDSHIFT_DB = 'dev'
REDSHIFT_USER = 'jenkins_user'
REDSHIFT_PASSWORD = 'im26c4u#uWu'

# API Details
BASE_URL = 'https://log10-api.loadshare.net'  # Add base URL for API endpoints
LOGIN_URL = f'{BASE_URL}/v1/login'
ACTUAL_API_URL = f'{BASE_URL}/lm/v1/drs/consignment/cod/retry'
HEADERS = {'Content-Type': 'application/json', 'deviceId': '123123123123'}
LOGIN_PAYLOAD = {
    "username": "vineeth.lsn",
    "password": "12345"
}
OUTPUT_FILE = "query_output.txt"

# Redshift Query
QUERY = """
SELECT cp.waybill_no
FROM raw_data.consignments_pod cp
LEFT JOIN raw_data.partner_transaction_payable ptp ON ptp.consignment_id = cp.consignment_id
WHERE cp.created_at >= dateadd(day, -4, current_date) 
  AND cp.created_at < current_date
  AND cp.payment_type IN ('CASH', 'cash') 
  AND cp.shipment_status = 'DEL' 
  AND cp.collected_amount > 0
  AND ptp.id IS NULL;
"""
# QUERY = """
# SELECT cp.waybill_no
# FROM raw_data.consignments_pod cp
# LEFT JOIN raw_data.partner_transaction_payable ptp ON ptp.consignment_id = cp.consignment_id
# WHERE cp.created_at >= dateadd(day, -4, current_date) 
#   AND cp.created_at < '2025-09-11 21:30:00'
#   AND cp.payment_type IN ('CASH', 'cash') 
#   AND cp.shipment_status = 'DEL' 
#   AND cp.collected_amount > 0
#   AND ptp.id IS NULL;"""

def fetch_waybills():
    """Fetch waybill numbers from Redshift."""
    try:
        conn = psycopg2.connect(
            dbname=REDSHIFT_DB,
            user=REDSHIFT_USER,
            password=REDSHIFT_PASSWORD,
            host=REDSHIFT_HOST,  
            port=REDSHIFT_PORT    
        )
        cursor = conn.cursor()
        cursor.execute(QUERY)
        waybills = [row[0] for row in cursor.fetchall()]
        
        with open(OUTPUT_FILE, "w") as f:
            for waybill in waybills:
                f.write(waybill + "\n")

        cursor.close()
        conn.close()
        print(f"Fetched {len(waybills)} waybills from Redshift.")
        return waybills
    except Exception as e:
        print(f"Error fetching data from Redshift: {e}")
        return []

def login():
    """Logs in and retrieves token."""
    try:
        response = requests.post(LOGIN_URL, headers=HEADERS, json=LOGIN_PAYLOAD)
        response.raise_for_status()  # Raise exception for HTTP errors
        login_response = response.json()
        # Adjust keys based on actual API response structure
        token = login_response.get('response', {}).get('token', {}).get('accessToken')
        tokenId = login_response.get('response', {}).get('token', {}).get('tokenId')
        if token and tokenId:
            return token, tokenId
        else:
            print("Token data missing in login response.")
            return None, None
    except Exception as e:
        print(f"Login failed: {e}")
        return None, None

def call_api(token, tokenId, waybills):
    """Calls actual API with authorization headers."""
    if not waybills:
        print("No waybills to process. Skipping API call.")
        return

    headers = HEADERS.copy()
    headers.update({'token': token, 'tokenId': tokenId})
    
    try:
        # Adjust payload structure based on API requirements
        payload = waybills # Example payload format
        print(payload)
        response = requests.post(ACTUAL_API_URL, headers=headers, json=payload)
        print(f"API Response Code: {response.status_code}")
        print(f"API Response Body: {response.text}")
    except Exception as e:
        print(f"Error calling API: {e}")

def main():
    waybills = fetch_waybills()
    count = len(waybills)

    if count > 1000:
        print("Exceeded waybill count limit (1000). Aborting further processing.")
        return

    token, tokenId = login()
    if token and tokenId:
        call_api(token, tokenId, waybills)
    else:
        print("Failed to authenticate. API call aborted.")

if __name__ == "__main__":
    main()

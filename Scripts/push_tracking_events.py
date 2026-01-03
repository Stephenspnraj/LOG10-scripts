import csv
import json
import requests
import pymysql
import sys
import time
from datetime import datetime

# ---------- CONFIG ----------
LOGIN_URL = "https://log10-api.loadshare.net/v1/login"
USERNAME = 'vineeth.lsn'
PASSWORD = '12345'
DEVICE_ID = '123123123123'

API_URL = "https://log10-api.loadshare.net/b2b/v1/tracking/push"
BATCH_SIZE = 250                # Waybill batch size
BATCH_SLEEP_SECONDS = 1.0        # Sleep for 1 second between batches
MAX_TRACKING_PER_CALL = 100     # Tracking IDs per API call
INPUT_FILE = "tracking_push.csv"
OUTFILE_PATH = "tracking_output.csv"

# ---------- DB CONFIG ----------
mysql_host = 'log10-tracking-replica.cco3osxqlq4g.ap-south-1.rds.amazonaws.com'
mysql_port = 3306
mysql_user = 'tracking_read_only'
mysql_password = 's&df90JrU6%k'
mysql_db = 'loadshare'

# ---------- FUNCTIONS ----------
def timestamp():
    """Return current timestamp string"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def get_token():
    """Login API to fetch token and tokenId"""
    try:
        login_headers = {
            'Content-Type': 'application/json',
            'deviceId': DEVICE_ID
        }
        login_data = {
            "username": USERNAME,
            "password": PASSWORD
        }
        response = requests.post(LOGIN_URL, headers=login_headers, json=login_data, timeout=20)
        response.raise_for_status()
        login_response = response.json()
        token = login_response['response']['token']['accessToken']
        tokenId = login_response['response']['token']['tokenId']
        return token, tokenId
    except Exception as e:
        print(f"{timestamp()} ❌ Login failed: {e}")
        return None, None

def fetch_ids_from_db(waybills, event_names):
    """Fetch ids from DB for given waybills and multiple event_names"""
    placeholders_wb = ','.join(['%s'] * len(waybills))
    placeholders_ev = ','.join(['%s'] * len(event_names))

    query = f"""
        SELECT id, waybill_number AS waybill, event_type
        FROM consignment_tracking
        WHERE waybill_number IN ({placeholders_wb})
        AND event_type IN ({placeholders_ev})
    """

    results = []
    conn = None
    try:
        conn = pymysql.connect(
            host=mysql_host,
            port=mysql_port,
            user=mysql_user,
            password=mysql_password,
            db=mysql_db,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        with conn.cursor() as cursor:
            cursor.execute(query, waybills + event_names)
            results = cursor.fetchall()
    except Exception as e:
        print(f"{timestamp()} ❌ DB query failed: {e}")
    finally:
        if conn:
            conn.close()
    return results

def chunk_list(lst, chunk_size):
    """Yield successive chunk_size-sized chunks from lst"""
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]

def process_batches(waybills, event_names, token, tokenId):
    """Process waybills in batches and call API"""
    total = len(waybills)
    total_batches = (total // BATCH_SIZE) + (1 if total % BATCH_SIZE != 0 else 0)

    # Track summary
    success_count, fail_count, no_db_count = 0, 0, 0

    with open(OUTFILE_PATH, mode='w', newline='') as outfile:
        writer = csv.writer(outfile)
        # Write header row
        writer.writerow(["batch_id", "api_call_no", "waybill_no", "event_name", "tracking_id", "remarks"])

        for idx in range(0, total, BATCH_SIZE):
            batch = waybills[idx:idx+BATCH_SIZE]
            batch_id = idx // BATCH_SIZE + 1

            # Fetch IDs for waybills+events
            records = fetch_ids_from_db(batch, event_names)

            # Build lookup of (waybill,event) -> DB row(s)
            db_map = {(r["waybill"], r["event_type"]): r for r in records}

            # Case: no data for this batch at all
            if not records:
                print(f"{timestamp()} ⚠️ Batch {batch_id}/{total_batches}: No IDs found for waybills {batch[0]} ... {batch[-1]}")
                for wb in batch:
                    for ev in event_names:
                        writer.writerow([batch_id, "-", wb, ev, "-", "No DB records found for given events"])
                        no_db_count += 1
                # Add sleep after processing each batch (except the last one)
                if idx + BATCH_SIZE < total:
                    print(f"{timestamp()} ⏳ Sleeping for {BATCH_SLEEP_SECONDS} seconds before next batch...")
                    time.sleep(BATCH_SLEEP_SECONDS)
                continue

            all_ids = [r["id"] for r in records]

            first_wb, last_wb = batch[0], batch[-1]
            print(f"\n{timestamp()} ➡️ Processing batch {batch_id}/{total_batches} "
                  f"({len(batch)} waybills, {len(all_ids)} tracking_ids) [{first_wb} ... {last_wb}]")

            # Split tracking IDs into safe sub-batches
            for api_call_no, sub_ids in enumerate(chunk_list(all_ids, MAX_TRACKING_PER_CALL), start=1):
                payload = {
                    "data": [{"trackingId": id_, "consignmentStatus": "NONE"} for id_ in sub_ids],
                    "delay": 1
                }

                try:
                    headers = {
                        "token": token,
                        "tokenId": tokenId,
                        "Content-Type": "application/json"
                    }
                    response = requests.post(API_URL, headers=headers, json=payload, timeout=45)

                    http_status = response.status_code
                    api_code, api_message, business_status = None, None, None

                    try:
                        resp_json = response.json()
                        api_code = resp_json.get("status", {}).get("code")
                        api_message = resp_json.get("status", {}).get("message")
                        business_status = resp_json.get("response", {}).get("status")
                    except Exception:
                        pass

                    if http_status == 200 and api_code == 200 and str(business_status).lower() == "success":
                        console_msg = (f"{timestamp()} ✅ API Response (batch {batch_id}, call {api_call_no}): "
                                       f"HTTP={http_status}, API_CODE={api_code}, API_MESSAGE={api_message}, BUSINESS_STATUS={business_status}")
                        csv_remarks = f"HTTP={http_status}, API_CODE={api_code}, API_MESSAGE={api_message}, BUSINESS_STATUS={business_status}"
                        success_count += len(sub_ids)
                    else:
                        console_msg = (f"{timestamp()} ❌ API Response (batch {batch_id}, call {api_call_no}): "
                                       f"HTTP={http_status}, API_CODE={api_code}, API_MESSAGE={api_message}, BUSINESS_STATUS={business_status}")
                        csv_remarks = f"HTTP={http_status}, API_CODE={api_code}, API_MESSAGE={api_message}, BUSINESS_STATUS={business_status}"
                        fail_count += len(sub_ids)

                    print(console_msg)

                    for r in records:
                        if r["id"] in sub_ids:
                            writer.writerow([batch_id, api_call_no, r['waybill'], r['event_type'], r['id'], csv_remarks])

                except Exception as e:
                    console_msg = f"{timestamp()} ❌ Exception during API call (batch {batch_id}, call {api_call_no}): {e}"
                    csv_remarks = f"Exception during API call: {e}"
                    print(console_msg)
                    fail_count += len(sub_ids)
                    for r in records:
                        if r["id"] in sub_ids:
                            writer.writerow([batch_id, api_call_no, r['waybill'], r['event_type'], r['id'], csv_remarks])
                    continue

            # Now handle waybill+event pairs missing from DB
            for wb in batch:
                for ev in event_names:
                    if (wb, ev) not in db_map:
                        writer.writerow([batch_id, "-", wb, ev, "-", "No DB records found for given events"])
                        no_db_count += 1
                        #print(f"{timestamp()} ⚠️ No DB record for waybill={wb}, event={ev} (batch {batch_id})")

            # Add sleep after processing each batch (except the last one)
            if idx + BATCH_SIZE < total:
                print(f"{timestamp()} ⏳ Sleeping for {BATCH_SLEEP_SECONDS} seconds before next batch...")
                time.sleep(BATCH_SLEEP_SECONDS)

    # Print summary
    print(f"\n==== SUMMARY ====")
    print(f"Total waybills processed: {total}")
    print(f"Success: {success_count}")
    print(f"Failed: {fail_count}")
    print(f"No DB Records: {no_db_count}")
    print(f"=================\n")

    print(f"{timestamp()} 🎯 Processing complete.")

# ---------- MAIN ----------
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"{timestamp()} ❌ Usage: python script.py EVENT1,EVENT2,...")
        sys.exit(1)

    # Step 1: Parse event names from CLI
    event_names = sys.argv[1].split(",")
    print(f"{timestamp()} 📌 Event types to process: {event_names}")

    # Step 2: Login and get tokens
    token, tokenId = get_token()
    if not token:
        sys.exit(1)

    # Step 3: Read waybills from input file
    # waybills = []
    # with open(INPUT_FILE, mode='r') as file:
    #     csv_reader = csv.DictReader(file)
    #     for row in csv_reader:
    #         waybills.append(row["waybill"])
    # Step 3: Read waybills from input file
    # waybills = []
    # with open(INPUT_FILE, mode='r') as file:
    #     csv_reader = csv.DictReader(file)
    
    #     # Clean up headers (strip spaces, BOM, etc.)
    #     csv_reader.fieldnames = [name.strip() for name in csv_reader.fieldnames]
    
    #     # Log headers for debugging in Jenkins
    #     print(f" CSV Headers detected: {csv_reader.fieldnames}")
    
    #     for row in csv_reader:
    #         waybills.append(row["waybill"].strip())
    
    # print(f"Total waybills loaded: {len(waybills)}")

    waybills = []
    with open(INPUT_FILE, mode='r', encoding="utf-8-sig") as file:
        csv_reader = csv.DictReader(file)
    
        # Clean headers
        csv_reader.fieldnames = [name.strip() for name in csv_reader.fieldnames]
        print(f"📌 CSV Headers detected: {csv_reader.fieldnames}")
    
        for row in csv_reader:
            waybills.append(row["waybill"].strip())
    
    print(f"✅ Total waybills loaded: {len(waybills)}")



    if not waybills:
        print(f"{timestamp()} ❌ No waybills found in input file.")
        sys.exit(1)

    print(f"{timestamp()} 📦 Total waybills loaded: {len(waybills)}")

    # Step 4: Process in batches
    process_batches(waybills, event_names, token, tokenId)

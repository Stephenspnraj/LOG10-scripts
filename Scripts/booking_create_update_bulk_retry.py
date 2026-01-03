import os
import csv
import re

import pymysql
import requests
import json
from json.decoder import JSONDecodeError

# Database configuration
mysql_host = 'log-10-replica-backup.cco3osxqlq4g.ap-south-1.rds.amazonaws.com'
mysql_port = 3306
mysql_user = 'log10_scripts'
mysql_password = 'D2lx7Wz0Wm9ISAY-Vp1gxKDTzLRRl5k1m'
mysql_db = 'loadshare'

# SQL query
sql_query = """
select waybill_no, request, request_type, checksum from
(select
    row_number() over (partition by waybill_no, request_type order by id desc) as rn,
    waybill_no,
    request,
    request_type,
    checksum
from external_api_logs
where waybill_no = %s) as temp
where rn = 1
"""

# Input and output files
input_csv = 'input.csv'
output_csv = 'output.csv'

def fetch_data(input_file):
    conn = pymysql.connect(
        host=mysql_host,
        port=mysql_port,
        user=mysql_user,
        password=mysql_password,
        db=mysql_db,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True
    )

    waybill_data = []

    try:
        with open(input_file, 'r') as csv_file:
            csv_reader = csv.reader(csv_file)
            next(csv_reader)  # Skip header
            for row in csv_reader:
                waybill_no = row[0]
                with conn.cursor() as cursor:
                    cursor.execute(sql_query, (waybill_no,))
                    results = cursor.fetchall()
                    for result in results:
                        try:
                            # Replace newlines (\r\n, \r, \n) with a space
                            payload = re.sub(r"\r\n|\r|\n", " ", result['request'])

                            # Replace non-ASCII characters with '?'
                            payload = re.sub(r"[^\x00-\x7F]", "?", payload)

                            payload = json.loads(payload)

                            waybill_data.append({
                                'waybill_no': result['waybill_no'],
                                'request_type': result['request_type'],
                                'checksum': result['checksum'],
                                'payload': payload,
                                'json_error': False
                            })
                        except JSONDecodeError:
                            print(f"❌ JSON error for waybill {result['waybill_no']}")
                            try:
                                # Regex to replace categories value
                                pattern = r'("categories":\s*)\[.*?\]'

                                replacement = lambda m: f'{m.group(1)}["{result["waybill_no"]}"]'

                                cleaned_json = re.sub(pattern, replacement, result['request'], flags=re.DOTALL)

                                print(f" Parsed JSON {cleaned_json}")

                                waybill_data.append({
                                    'waybill_no': result['waybill_no'],
                                    'request_type': result['request_type'],
                                    'checksum': result['checksum'],
                                    'payload': json.loads(cleaned_json),
                                    'json_error': False
                                })
                            except JSONDecodeError:
                                print(f"❌ Second layer JSON error for waybill {result['waybill_no']}")
                                waybill_data.append({
                                    'waybill_no': result['waybill_no'],
                                    'request_type': result['request_type'],
                                    'checksum': result['checksum'],
                                    'payload': None,
                                    'json_error': True
                                })
    finally:
        conn.close()

    return waybill_data

def send_requests_and_write_output(data, output_file):
    if os.path.exists(output_file):
        os.remove(output_file)

    total = len(data)

    with open(output_file, 'w', newline='') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(['waybillNo', 'requestType', 'isProcessed', 'exceptionMessage'])

        for index, item in enumerate(data, start=1):
            waybill_no = item['waybill_no']
            request_type = item['request_type']

            print(f"Processing waybill {index} of {total} → {waybill_no}")

            if item.get('json_error'):
                writer.writerow([waybill_no, request_type, 'False', 'Invalid Request Body'])
                continue

            payload = item['payload']

            # Determine endpoint
            if request_type == 'BOOKING_REARCH':
                url = 'https://meesho-api.loadshare.net/tp/v1/bookings'
            elif request_type == 'UPDATE_BOOKING_REARCH':
                url = 'https://meesho-api.loadshare.net/tp/v1/bookings/update'
            else:
                print(f"⚠️ Unknown request type for {waybill_no}: {request_type}")
                writer.writerow([waybill_no, request_type, 'False', 'Unknown request type'])
                continue

            headers = {
                'Content-Type': 'application/json',
                'checksum': item.get('checksum', '')
            }

            try:
                response = requests.post(url, headers=headers, json=payload)
                response_json = response.json()
                status_code = response_json.get('status', {}).get('code', 0)

                if status_code == 200:
                    data_list = response_json.get('response', {}).get('data', [])
                    if data_list:
                        entry = data_list[0]
                        is_processed = entry.get('isProcessed', False)
                        exception_message = entry.get('exceptionMessage', '') if not is_processed else ''
                        writer.writerow([waybill_no, request_type, str(is_processed), exception_message])
                    else:
                        writer.writerow([waybill_no, request_type, 'False', 'Empty response data'])
                else:
                    error_msg = response_json.get('status', {}).get('message', 'Unknown error')
                    writer.writerow([waybill_no, request_type, 'False', error_msg])

            except Exception as e:
                print(f"❌ Request failed for {waybill_no}: {e}")
                writer.writerow([waybill_no, request_type, 'False', str(e)])

def main():
    waybill_data = fetch_data(input_csv)
    send_requests_and_write_output(waybill_data, output_csv)
    print(f"✅ Output written to {output_csv}")

if __name__ == "__main__":
    main()

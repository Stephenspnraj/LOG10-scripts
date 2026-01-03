import os
import csv
import pymysql
import requests
import json
from datetime import datetime

# Database configuration
mysql_host = 'log-10-replica-backup.cco3osxqlq4g.ap-south-1.rds.amazonaws.com'
mysql_port = 3306
mysql_user = 'log10_scripts'
mysql_password = 'D2lx7Wz0Wm9ISAY-Vp1gxKDTzLRRl5k1m'
mysql_db = 'loadshare'

# Define the SQL query
sql_query = """
SELECT checksum, request AS payload FROM external_api_logs WHERE  waybill_no = %s
GROUP BY waybill_no
"""

# Input and output files
input_csv = 'input.csv'
output_csv = 'output.csv'

# Function to fetch data from the database and write to CSV
def fetch_data_to_csv(input_file, output_file):
    # Remove output file if it exists
    if os.path.exists(output_file):
        os.remove(output_file)
    
    # Connect to the database
    conn = pymysql.connect(
        host=mysql_host,
        port=mysql_port,
        user=mysql_user,
        password=mysql_password,
        db=mysql_db,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )
    
    try:
        # Open the input CSV file and read waybill_no
        with open(input_file, 'r') as csv_file:
            csv_reader = csv.reader(csv_file)
            next(csv_reader)  # Skip header row
            
            with open(output_file, 'w', newline='') as outfile:
                writer = csv.writer(outfile)
                first_waybill = True  # Flag to indicate if it's the first waybill
                total_waybills = sum(1 for _ in csv_reader)  # Count total number of waybills
                csv_file.seek(0)  # Reset file pointer to start
                next(csv_reader)  # Skip header row
                
                for index, row in enumerate(csv_reader, 1):
                    waybill_no = row[0]

                    # Execute the SQL query for the current waybill_no
                    with conn.cursor() as cursor:
                        cursor.execute(sql_query, (waybill_no,))
                        query_result = cursor.fetchall()

                        if query_result:
                            if first_waybill:
                                # Write the column headers to the output file only for the first waybill
                                writer.writerow(query_result[0].keys())
                                first_waybill = False

                            # Write the query result to the output file
                            for result in query_result:
                                writer.writerow(result.values())

                    # Print progress
                    print(f"Processed waybill {index}/{total_waybills} → {waybill_no}")
                    print(f"Progress: {index/total_waybills * 100:.2f}%\n")
    finally:
        # Close MySQL connection
        conn.close()

    print(f"Output file created: {output_file}")

# Function to read CSV file and return list of checksums and payloads
def read_csv(file_path):
    checksums = []
    payloads = []
    with open(file_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            checksums.append(row['checksum'])
            payloads.append(json.loads(row['payload']))
    return checksums, payloads

# Function to send POST request for each checksum and payload
def send_post_requests(checksums, payloads):
    url = 'https://meesho-api.loadshare.net/tp/v1/bookings'
    for checksum, payload in zip(checksums, payloads):
        headers = {
            'Content-Type': 'application/json',
            'checksum': checksum
        }
        response = requests.post(url, headers=headers, json=payload)
        print("Response:", response.text)
        # Uncomment the next two lines to add a delay between requests
        # time.sleep(1)
        # print("Sleeping 1 second")

# Main function
def main():
    # Fetch data from the database and write to CSV
    fetch_data_to_csv(input_csv, output_csv)

    # Read checksums and payloads from the generated CSV
    checksums, payloads = read_csv(output_csv)

    # Send POST requests for each checksum and payload
    send_post_requests(checksums, payloads)

if __name__ == "__main__":
    main()

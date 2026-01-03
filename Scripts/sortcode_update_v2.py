import csv
import pymysql
from tabulate import tabulate
from colorama import init, Fore, Style

# Database credentials (hardcoded as per current practice)
# mysql_host = 'log10-staging.cco3osxqlq4g.ap-south-1.rds.amazonaws.com'
# mysql_port = 3306  # 
# mysql_user = 'log10_staging'
# mysql_password = 'A_edjsHKmDF6vajhL4go6ekP'
# mysql_db = 'loadshare'
mysql_host = 'log10-replica-replica.cco3osxqlq4g.ap-south-1.rds.amazonaws.com'
mysql_port = 3306  
mysql_user = 'log10_scripts'
mysql_password = 'D2lx7Wz0Wm9ISAY-Vp1gxKDTzLRRl5k1m'
mysql_db = 'loadshare'


REQUIRED_HEADERS = ['LMDC', 'Current Sort Code', 'New Sort Code']
INPUT_CSV = "sortcode_update_input.csv"             
OUTPUT_CSV = "sortcode_update_outfile.csv"         

def get_mysql_connection():
    return pymysql.connect(
        host=mysql_host,
        port=mysql_port,
        user=mysql_user,
        password=mysql_password,
        db=mysql_db,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

def normalize_headers(headers):
    return {h.strip().lower(): h for h in headers}

def get_location_id(cursor, column, value):
    cursor.execute(f"SELECT id FROM locations WHERE {column} = %s", (value,))
    result = cursor.fetchone()
    return result["id"] if result else None

def print_query_result(cursor, query, params=None):
    cursor.execute(query, params or ())
    rows = cursor.fetchall()
    if rows:
        print(tabulate(rows, headers="keys", tablefmt="pretty"))
    else:
        print("No results found.")

def process_csv(input_path, output_path):
    init(autoreset=True)
    success = 0
    skipped_validation = 0
    errors = 0
    total = 0

    with open(input_path, newline='', encoding='utf-8') as csvfile, \
         open(output_path, 'w', newline='', encoding='utf-8') as outfile:

        reader = csv.DictReader(csvfile)
        if not reader.fieldnames:
            print(Fore.RED + "Input CSV has no headers.")
            return

        headers_map = normalize_headers(reader.fieldnames)
        for col in REQUIRED_HEADERS:
            if col.lower() not in headers_map:
                print(Fore.RED + f"Missing required column: {col}")
                return

        out_fieldnames = list(reader.fieldnames)
        if 'Remarks' not in [h.strip() for h in out_fieldnames]:
            out_fieldnames.append('Remarks')

        writer = csv.DictWriter(outfile, fieldnames=out_fieldnames)
        writer.writeheader()

        conn = None
        cursor = None
        try:
            conn = get_mysql_connection()
            cursor = conn.cursor()

            for i, row in enumerate(reader, 1):
                total += 1
                remarks = []
                try:
                    lmdc = row[headers_map['lmdc']].strip()
                    current_sort_code = row[headers_map['current sort code']].strip()
                    new_sort_code = row[headers_map['new sort code']].strip()
                    print(Fore.CYAN + f"\nProcessing row {i}: [{lmdc}, {current_sort_code}, {new_sort_code}]")

                    # Validation: LMDC exists
                    lmdc_id = get_location_id(cursor, 'client_location_name', lmdc)
                    if not lmdc_id:
                        msg = f"Validation failed: LMDC ({lmdc}) not found."
                        print(Fore.YELLOW + msg)
                        remarks.append(msg)
                        skipped_validation += 1
                        row['Remarks'] = "; ".join(remarks)
                        # writer.writerow(row)
                        continue

                    # Validation: LMDC must appear in New Sort Code (case-insensitive)
                    if lmdc.lower() not in new_sort_code.lower():
                        msg = f"Validation failed: LMDC ({lmdc}) not in New Sort Code ({new_sort_code})."
                        print(Fore.YELLOW + msg)
                        remarks.append(msg)
                        skipped_validation += 1
                        row['Remarks'] = "; ".join(remarks)
                        # writer.writerow(row)
                        continue

                    # Step 1: Before network_metadata update
                    print("Before Network Metadata Update:")
                    print_query_result(
                        cursor,
                        "SELECT id,location_alias,next_location_alias,crossdock_alias,is_active,updated_at "
                        "FROM network_metadata WHERE next_location_alias IN "
                        "(SELECT alias FROM locations WHERE client_location_name = %s)",
                        (lmdc,)
                    )

                    # Step 2: Update network_metadata
                    print(f"Executing query: UPDATE network_metadata SET next_location_alias = '{new_sort_code}' "
                          f"WHERE next_location_alias IN (SELECT alias FROM locations WHERE client_location_name = '{lmdc}')")
                    cursor.execute(
                        "UPDATE network_metadata SET next_location_alias = %s WHERE next_location_alias IN "
                        "(SELECT alias FROM locations WHERE client_location_name = %s)",
                        (new_sort_code, lmdc)
                    )
                    conn.commit()

                    # Step 3: Before location update and then update
                    print("Before Location Update:")
                    print_query_result(
                        cursor,
                        "SELECT id,alias,client_location_name,updated_at FROM locations WHERE client_location_name = %s",
                        (lmdc,)
                    )
                    print(f"Executing query: UPDATE locations SET alias = '{new_sort_code}' WHERE client_location_name = '{lmdc}'")
                    cursor.execute(
                        "UPDATE locations SET alias = %s WHERE client_location_name = %s",
                        (new_sort_code, lmdc)
                    )
                    conn.commit()

                    remarks.append("Success")
                    success += 1

                except Exception as e:
                    msg = f"Error processing row {i}: {e}"
                    print(Fore.RED + msg)
                    remarks.append(msg)
                    errors += 1
                finally:
                    row['Remarks'] = "; ".join(remarks) if remarks else ""
                    writer.writerow(row)

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    # Summary
    print(Style.BRIGHT + "\nSummary:")
    print(f"Total rows processed: {total}")
    print(Fore.GREEN + f"Successful updates: {success}")
    print(Fore.YELLOW + f"Skipped due to validation: {skipped_validation}")
    print(Fore.RED + f"Errored rows: {errors}")
    print("\nSort code update process completed.")
    print(f"Output written to: {output_path}")

if __name__ == "__main__":
    process_csv(INPUT_CSV, OUTPUT_CSV)

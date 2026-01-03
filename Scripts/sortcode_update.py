import csv
import pymysql
from tabulate import tabulate
from colorama import init, Fore

# Database credentials
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


def process_csv(file_path):
    conn = get_mysql_connection()
    cursor = conn.cursor()
    init(autoreset=True)

    with open(file_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        headers_map = normalize_headers(reader.fieldnames)

        for col in REQUIRED_HEADERS:
            if col.lower() not in headers_map:
                print(f"Missing required column: {col}")
                return

        for i, row in enumerate(reader, 1):
            try:
                lmdc = row[headers_map['lmdc']].strip()
                current_sort_code = row[headers_map['current sort code']].strip()
                new_sort_code = row[headers_map['new sort code']].strip()

                print(Fore.CYAN + f"\nProcessing row {i}: [{lmdc}, {current_sort_code}, {new_sort_code}]\n")

                # Validation
                lmdc_id = get_location_id(cursor, 'client_location_name', lmdc)
                if not lmdc_id:
                    print(Fore.YELLOW + f"Validation failed: LMDC ({lmdc}) not found.")
                    continue

                # Step 1: Print current network_metadata state
                print("Before Network Metadata Update:")
                print_query_result(cursor, "SELECT id,location_alias,next_location_alias,crossdock_alias,is_active,updated_at FROM network_metadata WHERE next_location_alias IN (SELECT alias FROM locations WHERE client_location_name = %s)", (lmdc,))
                # print_query_result(cursor, """
                #     SELECT id, location_alias, next_location_alias, crossdock_alias, is_active, updated_at
                #     FROM network_metadata
                #     WHERE next_location_alias = %s
                # """, (current_sort_code,))

                # Step 2: Update network_metadata
                # print(f"Executing update: SET next_location_alias = '{lmdc}'")
                print(f"""Executing query:
                      UPDATE network_metadata SET next_location_alias = '{new_sort_code}'
    WHERE next_location_alias IN (SELECT alias FROM locations WHERE client_location_name = '{lmdc}')
    """) 
                cursor.execute("""
                    UPDATE network_metadata
                    SET  next_location_alias = %s
                    WHERE next_location_alias IN (SELECT alias FROM locations WHERE client_location_name = %s) 
                """, (new_sort_code, lmdc))
                conn.commit()

                # Step 3: Print current locations state
                print("Before Location Update:")
                #print(Fore.YELLOW + "Before Location Update:" + Style.RESET_ALL)
                print_query_result(cursor, "SELECT id,alias,client_location_name,updated_at FROM locations WHERE client_location_name = %s", (lmdc,))
                print(f"Executing query: UPDATE locations SET alias = '{new_sort_code}' WHERE client_location_name = '{lmdc}'")
                cursor.execute("UPDATE locations SET alias = %s WHERE client_location_name = %s", (new_sort_code, lmdc))
                conn.commit()

            except Exception as e:
                print(Fore.RED + f"Error processing row {i}: {e}")
                continue

    cursor.close()
    conn.close()
    print("\nSort code update process completed.")


if __name__ == "__main__":
    csv_file_path = "sortcode_update.csv"  # Or use absolute path
    process_csv(csv_file_path)

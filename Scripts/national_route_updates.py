import pymysql
import pandas as pd
from tabulate import tabulate

# MySQL Connection Details
# mysql_host = 'log10-staging.cco3osxqlq4g.ap-south-1.rds.amazonaws.com'
# mysql_port = 3306  # Assuming default MySQL port
# mysql_user = 'log10_staging'
# mysql_password = 'A_edjsHKmDF6vajhL4go6ekP'
# mysql_db = 'loadshare'
mysql_host = 'log10-replica-replica.cco3osxqlq4g.ap-south-1.rds.amazonaws.com'
mysql_port = 3306  
mysql_user = 'log10_scripts'
mysql_password = 'D2lx7Wz0Wm9ISAY-Vp1gxKDTzLRRl5k1m'
mysql_db = 'loadshare'

# Connect to MySQL
conn = pymysql.connect(
    host=mysql_host,
    port=mysql_port,
    user=mysql_user,
    password=mysql_password,
    db=mysql_db,
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor
)

def update_manifests(conn, location, destination_location, new_next_alias):
    # Extract base parts for all aliases
    base_location = location.split('.')[0]
    base_destination = destination_location.split('.')[0]
    base_new_next = new_next_alias.split('.')[0]  # Extract base from new_next_alias

    # Select pending manifests using base aliases
    select_query = f"""
    SELECT 
        m.manifest_code, 
        cl.alias AS manifest_current_loc_name, 
        nl.alias AS next_loc_name, 
        dl.alias AS destination_loc_name, 
        m.flow_type, 
        m.updated_at 
    FROM 
        manifests m 
        JOIN locations ol ON ol.id = m.originated_loc_id 
        JOIN locations cl ON cl.id = m.current_loc_id 
        JOIN locations dl ON dl.id = m.destination_loc_id
        JOIN locations nl ON nl.id = m.next_loc_id 
    WHERE 
        cl.alias = '{base_location}' 
        AND dl.alias = '{base_destination}' 
        AND m.manifest_status = 'PENDING' 
        AND m.is_active = 1 
        AND m.updated_at > NOW() - INTERVAL 30 DAY 
    GROUP BY 
        m.manifest_code, 
        ol.alias, 
        cl.alias, 
        m.manifest_status, 
        m.flow_type 
    ORDER BY 
        m.updated_at DESC;
    """
    #print("\nExecuting Select Query for Pending Manifests:")
    #print(select_query)
    with conn.cursor() as cursor:
        cursor.execute(select_query)
        results = cursor.fetchall()
        print(f"Found {len(results)} pending manifests.")
        if results:
            print(tabulate(results, headers="keys", tablefmt="pretty"))
        else:
            print("No pending manifests found.")
    
    if not results:
        #print("No manifests to update for this case.")
        return 0
    
    # Get new next location ID using base alias
    get_next_loc_id_query = f"SELECT id FROM locations WHERE alias = '{base_new_next}'"
    print("\nExecuting Query to Get New Next Location ID:")
    print(get_next_loc_id_query)
    with conn.cursor() as cursor:
        cursor.execute(get_next_loc_id_query)
        next_loc = cursor.fetchone()
        if not next_loc:
            print(f"Error: Location alias '{base_new_next}' not found.")
            return 0
        next_loc_id = next_loc['id']
    
    # Update manifests
    manifest_codes = [result['manifest_code'] for result in results]
    codes_str = ','.join([f"'{code}'" for code in manifest_codes])
    update_query = f"""
    UPDATE manifests 
    SET next_loc_id = {next_loc_id} 
    WHERE manifest_code IN ({codes_str})
    """
    print("\nExecuting Update Query:")
    print(update_query)
    with conn.cursor() as cursor:
        cursor.execute(update_query)
        updated_rows = cursor.rowcount
        conn.commit()
        print(f"Successfully updated {updated_rows} manifests.")
    
    return updated_rows

# Load the CSV file
csv_file = "inputfile.csv"
#csv_file = "/Users/Lsn-Santoshdev/Downloads/linehaul.csv"
df = pd.read_csv(csv_file)
column_map = {col.lower(): col for col in df.columns}
# Verify required columns exist
required_columns = ['fmsc', 'fmcd', 'lmcd', 'lmsc']
missing = [col for col in required_columns if col not in column_map]
if missing:
    raise ValueError(f"Missing required columns: {[col.upper() for col in missing]}")
# Check for manifest correction column (case-insensitive)
manifest_correction_column = 'is_manifest_correction_required'.lower()
has_correction_column = manifest_correction_column in column_map

# Iterate through each row in the CSV
for index, row in df.iterrows():
    FMSC = row[column_map['fmsc']]
    FMCD = row[column_map['fmcd']]
    LMCD = row[column_map['lmcd']]
    LMSC = row[column_map['lmsc']]
    


    location_alias = f"{FMSC}.FMSC" if pd.notna(FMSC) else None
    next_location_alias = f"{LMSC}.LMSC" if pd.notna(LMSC) else None
    crossdock_alias = None

    #print("\nProcessing Row:", row.to_dict())  # Debugging Output
    print(f"\nProcessing Row {index + 1}:\n\n{row.to_dict()}")

            # Check if manifest correction is needed (if column exists)
    # if has_correction_column:
    #     correction_value = str(row[manifest_correction_column]).lower().strip()
    #     needs_correction = correction_value in {'yes', 'true', '1'}
    #     print(f"\nManifest Correction Required?: {'YES' if needs_correction else 'NO'}\n\n")
    # else:
    #     # If column doesn't exist, default to True (original behavior)
    #     #needs_correction = True
    #     needs_correction = False
    # Check manifest correction requirements
    if has_correction_column:
        # Get the raw value from the CSV
        raw_value = row[column_map[manifest_correction_column]]
        
        # Handle null/NaN values first
        if pd.isna(raw_value):
            needs_correction = False
        else:
            # Process non-null values
            correction_value = str(raw_value).lower().strip()
            needs_correction = correction_value in {'yes', 'true', '1'}
            
            # Explicitly handle 'no'/'false'/'0' cases
            if correction_value in {'no', 'false', '0'}:
                needs_correction = False
    else:
        # If column doesn't exist, don't update manifests
        needs_correction = False

    print(f"\nManifest Correction Required?: {'YES' if needs_correction else 'NO'}")

    # Case 1: Both FMCD and LMCD are empty
    if pd.isna(FMCD) and pd.isna(LMCD):
        print("Executing Case 1: Both FMCD and LMCD are NULL")

        # Select existing mapping
        select_query = f"select id,location_alias,next_location_alias,crossdock_alias,is_active,updated_at from network_metadata where location_alias='{location_alias}' AND next_location_alias='{next_location_alias}' AND is_active=1"
        print("Executing:", select_query)
        with conn.cursor() as cursor:
            cursor.execute(select_query)
            result = cursor.fetchall()
            print(tabulate(result, headers="keys", tablefmt="pretty"))

        # Deactivate existing mapping
        update_query = f"UPDATE network_metadata SET is_active=0 WHERE location_alias='{location_alias}' AND next_location_alias='{next_location_alias}' AND is_active=1"
        print("Executing:", update_query)
        with conn.cursor() as cursor:
            cursor.execute(update_query)
            conn.commit()

        # Check before inserting
        check_query = f"SELECT id FROM network_metadata WHERE location_alias='{location_alias}' AND next_location_alias='{next_location_alias}' AND (crossdock_alias IS NULL OR crossdock_alias = '') AND is_active=0 order by id desc limit 1"
        with conn.cursor() as cursor:
            cursor.execute(check_query)
            existing_record = cursor.fetchone()

        if existing_record:
            select_active_query = f"select id,location_alias,next_location_alias,crossdock_alias,is_active,updated_at from network_metadata where id={existing_record['id']}"
            print("Executing existing records query:", select_active_query)
            with conn.cursor() as cursor:
                cursor.execute(select_active_query)
                result = cursor.fetchall()
                print(tabulate(result, headers="keys", tablefmt="pretty"))
            update_active_query = f"UPDATE network_metadata SET is_active=1 WHERE id={existing_record['id']}"
            print("Executing:", update_active_query)
            with conn.cursor() as cursor:
                cursor.execute(update_active_query)
                conn.commit()
        else:
            insert_query = f"INSERT INTO network_metadata (location_alias, next_location_alias, is_active, audit_log) VALUES ('{location_alias}', '{next_location_alias}', 1, 'migration_date')"
            print("Executing:", insert_query)
            with conn.cursor() as cursor:
                cursor.execute(insert_query)
                conn.commit()
        #print("\nUpdating Manifests for Case 1")
        #update_manifests(conn, location_alias, next_location_alias, next_location_alias)
                # Update manifests only if correction is needed
        if needs_correction:
            print("\nUpdating Manifests for Case 1")
            update_manifests(conn, location_alias, next_location_alias, next_location_alias)
        else:
            print("\nSkipping Manifest Update for Case 1 (correction not required)")
    # Case 2: FMCD is not NULL, LMCD is NULL
    elif pd.notna(FMCD) and pd.isna(LMCD):
        print("Executing Case 2: FMCD is not NULL, LMCD is NULL")
        crossdock_alias = f"{FMCD}.FMSC"

        select_query = f"select id,location_alias,next_location_alias,crossdock_alias,is_active,updated_at from network_metadata where location_alias='{location_alias}' AND next_location_alias='{next_location_alias}' AND is_active=1"
        print("Executing:", select_query)
        with conn.cursor() as cursor:
            cursor.execute(select_query)
            result = cursor.fetchall()
            print(tabulate(result, headers="keys", tablefmt="pretty"))

        update_query = f"UPDATE network_metadata SET is_active=0 WHERE location_alias='{location_alias}' AND next_location_alias='{next_location_alias}' AND is_active=1"
        print("Executing:", update_query)
        with conn.cursor() as cursor:
            cursor.execute(update_query)
            conn.commit()

        check_query = f"SELECT id FROM network_metadata WHERE location_alias='{location_alias}' AND next_location_alias='{next_location_alias}' AND crossdock_alias='{crossdock_alias}' AND is_active=0 order by id desc limit 1"
        with conn.cursor() as cursor:
            cursor.execute(check_query)
            existing_record = cursor.fetchone()

        if existing_record:
            select_active_query = f"select id,location_alias,next_location_alias,crossdock_alias,is_active,updated_at from network_metadata where id={existing_record['id']}"
            print("Executing existing records query:", select_active_query)
            with conn.cursor() as cursor:
                cursor.execute(select_active_query)
                result = cursor.fetchall()
                print(tabulate(result, headers="keys", tablefmt="pretty"))
            update_active_query = f"UPDATE network_metadata SET is_active=1 WHERE id={existing_record['id']}"
            print("Executing:", update_active_query)
            with conn.cursor() as cursor:
                cursor.execute(update_active_query)
                conn.commit()
        else:
            insert_query = f"INSERT INTO network_metadata (location_alias, next_location_alias, crossdock_alias, is_active, audit_log) VALUES ('{location_alias}', '{next_location_alias}', '{crossdock_alias}', 1, 'migration_date')"
            print("Executing:", insert_query)
            with conn.cursor() as cursor:
                cursor.execute(insert_query)
                conn.commit()
        #print("\nUpdating Manifests for Case 2")
        #update_manifests(conn, location_alias, next_location_alias, crossdock_alias)
        # Update manifests only if correction is needed
        if needs_correction:
            print("\nUpdating Manifests for Case 2")
            update_manifests(conn, location_alias, next_location_alias, crossdock_alias)
        else:
            print("\nSkipping Manifest Update for Case 2 (correction not required)")
    # Case 3: FMCD is NULL, LMCD is not NULL
    elif pd.isna(FMCD) and pd.notna(LMCD):
        print("Executing Case 3: FMCD is NULL, LMCD is not NULL")
        crossdock_alias = f"{LMCD}.FMSC"

        select_query = f"select id,location_alias,next_location_alias,crossdock_alias,is_active,updated_at from network_metadata where location_alias='{location_alias}' AND next_location_alias='{next_location_alias}' AND is_active=1"
        print("Executing:", select_query)
        with conn.cursor() as cursor:
            cursor.execute(select_query)
            result = cursor.fetchall()
            print(tabulate(result, headers="keys", tablefmt="pretty"))

        update_query = f"UPDATE network_metadata SET is_active=0 WHERE location_alias='{location_alias}' AND next_location_alias='{next_location_alias}' AND is_active=1"
        print("Executing:", update_query)
        with conn.cursor() as cursor:
            cursor.execute(update_query)
            conn.commit()

        check_query = f"SELECT id FROM network_metadata WHERE location_alias='{location_alias}' AND next_location_alias='{next_location_alias}' AND crossdock_alias='{crossdock_alias}' AND is_active=0 order by id desc limit 1"
        with conn.cursor() as cursor:
            cursor.execute(check_query)
            existing_record = cursor.fetchone()

        if existing_record:
            select_active_query = f"select id,location_alias,next_location_alias,crossdock_alias,is_active,updated_at from network_metadata where id={existing_record['id']}"
            print("Executing existing records query:", select_active_query)
            with conn.cursor() as cursor:
                cursor.execute(select_active_query)
                result = cursor.fetchall()
                print(tabulate(result, headers="keys", tablefmt="pretty"))
            update_active_query = f"UPDATE network_metadata SET is_active=1 WHERE id={existing_record['id']}"
            print("Executing:", update_active_query)
            with conn.cursor() as cursor:
                cursor.execute(update_active_query)
                conn.commit()
        else:
            insert_query = f"INSERT INTO network_metadata (location_alias, next_location_alias, crossdock_alias, is_active, audit_log) VALUES ('{location_alias}', '{next_location_alias}', '{crossdock_alias}', 1, 'migration_date')"
            print("Executing:", insert_query)
            with conn.cursor() as cursor:
                cursor.execute(insert_query)
                conn.commit()
        #print("\nUpdating Manifests for Case 3")
        #update_manifests(conn, location_alias, next_location_alias, crossdock_alias)
        # Update manifests only if correction is needed
        if needs_correction:
            print("\nUpdating Manifests for Case 3")
            update_manifests(conn, location_alias, next_location_alias, crossdock_alias)
        else:
            print("\nSkipping Manifest Update for Case 3 (correction not required)")
# Case 4: Both FMCD and LMCD are NOT NULL
    elif pd.notna(FMCD) and pd.notna(LMCD):
        print("Executing Case 4: Both FMCD and LMCD are NOT NULL")

        # Select existing mapping for both FMCD and LMCD
        for loc_alias in [location_alias, f"{FMCD}.FMSC"]:
            select_query = f"select id,location_alias,next_location_alias,crossdock_alias,is_active,updated_at from network_metadata where location_alias='{loc_alias}' AND next_location_alias='{next_location_alias}' AND is_active=1"
            print("Executing:", select_query)
            with conn.cursor() as cursor:
                cursor.execute(select_query)
                result = cursor.fetchall()
                print(tabulate(result, headers="keys", tablefmt="pretty"))

        # Deactivate both existing mappings
        for loc_alias in [location_alias, f"{FMCD}.FMSC"]:
            update_query = f"UPDATE network_metadata SET is_active=0 WHERE location_alias='{loc_alias}' AND next_location_alias='{next_location_alias}' AND is_active=1"
            print("Executing:", update_query)
            with conn.cursor() as cursor:
                cursor.execute(update_query)
                conn.commit()

        # Insert check before inserting Case 4 records
        check_query_1 = f"SELECT id FROM network_metadata WHERE location_alias='{location_alias}' AND next_location_alias='{next_location_alias}' AND crossdock_alias='{FMCD}.FMSC' AND is_active=0 order by id desc limit 1"
        with conn.cursor() as cursor:
            cursor.execute(check_query_1)
            existing_record_1 = cursor.fetchone()

        if existing_record_1:   
            select_active_query_1 = f"select id,location_alias,next_location_alias,crossdock_alias,is_active,updated_at from network_metadata where id={existing_record_1['id']}"
            print("Executing existing records query:", select_active_query_1)
            with conn.cursor() as cursor:
                cursor.execute(select_active_query_1)
                result = cursor.fetchall()
                print(tabulate(result, headers="keys", tablefmt="pretty"))
            update_active_query_1 = f"UPDATE network_metadata SET is_active=1 WHERE id={existing_record_1['id']}"
            print("Executing:", update_active_query_1)
            with conn.cursor() as cursor:
                cursor.execute(update_active_query_1)
                conn.commit()
        else:
            insert_query_1 = f"INSERT INTO network_metadata (location_alias, next_location_alias, crossdock_alias, is_active, audit_log) VALUES ('{location_alias}', '{next_location_alias}', '{FMCD}.FMSC', 1, 'migration_date')"
            print("Executing:", insert_query_1)
            with conn.cursor() as cursor:
                cursor.execute(insert_query_1)
                conn.commit()

        check_query_2 = f"SELECT id FROM network_metadata WHERE location_alias='{FMCD}.FMSC' AND next_location_alias='{next_location_alias}' AND crossdock_alias='{LMCD}.LMSC' AND is_active=0 order by id desc limit 1"
        with conn.cursor() as cursor:
            cursor.execute(check_query_2)
            existing_record_2 = cursor.fetchone()

        if existing_record_2:
            select_active_query_2 = f"select id,location_alias,next_location_alias,crossdock_alias,is_active,updated_at from network_metadata where id={existing_record_2['id']}"
            print("Executing existing records query:", select_active_query_2)
            with conn.cursor() as cursor:
                cursor.execute(select_active_query_2)
                result = cursor.fetchall()
                print(tabulate(result, headers="keys", tablefmt="pretty"))
            update_active_query_2 = f"UPDATE network_metadata SET is_active=1 WHERE id={existing_record_2['id']}"
            print("Executing:", update_active_query_2)
            with conn.cursor() as cursor:
                cursor.execute(update_active_query_2)
                conn.commit()
        else:
            insert_query_2 = f"INSERT INTO network_metadata (location_alias, next_location_alias, crossdock_alias, is_active, audit_log) VALUES ('{FMCD}.FMSC', '{next_location_alias}', '{LMCD}.LMSC', 1, 'migration_date')"
            print("Executing:", insert_query_2)
            with conn.cursor() as cursor:
                cursor.execute(insert_query_2)
                conn.commit()
        # Update manifests for Case 4 (two parts)
        # Update manifests only if correction is needed
        if needs_correction:
            print("\nUpdating Manifests for Case 4 (First Part)")
            update_manifests(conn, location_alias, next_location_alias, f"{FMCD}.FMSC")
            print("\nUpdating Manifests for Case 4 (Second Part)")
            update_manifests(conn, f"{FMCD}.FMSC", next_location_alias, f"{LMCD}.LMSC")
        else:
            print("\nSkipping Manifest Update for Case 4 (correction not required)")
# Close MySQL connection
conn.close()
print("\nScript execution completed!")

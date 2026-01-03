import pymysql
import pandas as pd

# Load the input file
file_path = 'entity_correction.csv'
df = pd.read_csv(file_path)

# Function to process entity_code and entity_type
def process_entity_code(cursor, entity_code, entity_type):
    if entity_type == 'ACTIVATE':
        # Select query for ACTIVATE
        select_query = f"SELECT * FROM users WHERE id='{entity_code}' AND role=3;"
        cursor.execute(select_query)
        result = cursor.fetchall()
        
        if result:
            # Update query for ACTIVATE
            update_query = f"UPDATE users SET status='APPROVED',is_active=1 WHERE id='{entity_code}' AND role=3;"
            cursor.execute(update_query)
            print(f"Updated {entity_code} to APPROVED.")
        else:
            print(f"No results found for {entity_type} with code {entity_code}.")
    
    elif entity_type == 'DEACTIVATE':
        # Select query for DEACTIVATE
        select_query = f"SELECT * FROM users WHERE id='{entity_code}' AND role=3;"
        cursor.execute(select_query)
        result = cursor.fetchall()
        
        if result:
            # Update query for DEACTIVATE
            update_query = f"UPDATE users SET status='DEACTIVATED',is_active=0 WHERE id='{entity_code}' AND role=3 ;"
            cursor.execute(update_query)
            print(f"Updated {entity_code} to DEACTIVATED.")
        else:
            print(f"No results found for {entity_type} with code {entity_code}.")
    
    elif entity_type == 'AADHAR':
        # Select query for AADHAR
        select_query = f"SELECT * FROM entity_documents WHERE doc_type='AADHAR' AND doc_number='{entity_code}' AND is_active=1;"
        cursor.execute(select_query)
        result = cursor.fetchall()
        
        if result:
            # Update query for AADHAR
            update_query = f"UPDATE entity_documents SET is_active=0 WHERE doc_type='AADHAR' AND doc_number='{entity_code}' AND is_active=1;"
            cursor.execute(update_query)
            print(f"Deactivated AADHAR {entity_code}.")
        else:
            print(f"No results found for {entity_type} with code {entity_code}.")
    
    elif entity_type == 'PANCARD':
        # Select query for PANCARD
        select_query = f"SELECT * FROM entity_documents WHERE doc_type='PANCARD' AND doc_number='{entity_code}' AND is_active=1;"
        cursor.execute(select_query)
        result = cursor.fetchall()
        
        if result:
            # Update query for PANCARD
            update_query = f"UPDATE entity_documents SET is_active=0 WHERE doc_type='PANCARD' AND doc_number='{entity_code}' AND is_active=1;"
            cursor.execute(update_query)
            print(f"Deactivated PANCARD {entity_code}.")
        else:
            print(f"No results found for {entity_type} with code {entity_code}.")
    
    else:
        # Invalid entity_type error message
        print(f"Invalid entity_type '{entity_type}', expected entity_type => ACTIVATE,DEACTIVATE,AADHAR,PANCARD.")

# MySQL connection details
# mysql_host = 'staging-mysql-mumbai.cco3osxqlq4g.ap-south-1.rds.amazonaws.com'
# mysql_port = 3306  # Assuming default MySQL port
# mysql_user = 'loadsharetest123'
# mysql_password = 'loadsharetest123'
# mysql_db = 'titan'
mysql_host = 'prod-titan-rds.cco3osxqlq4g.ap-south-1.rds.amazonaws.com'
mysql_port = 3306  # Assuming default MySQL port
mysql_user = 'prod_titan_app'
mysql_password = '7Xc8ZG0bEpFTkhrrGoRAlhaP5qV9zSkfEagQ'
mysql_db = 'titan'


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
    with conn.cursor() as cursor:
        # Loop through each row in the input file and process the entity_code and entity_type
        for _, row in df.iterrows():
            entity_code = row['entity_code']
            entity_type = row['entity_type']
            process_entity_code(cursor, entity_code, entity_type)
    
    # Commit changes to the database
    conn.commit()

finally:
    # Close the connection
    conn.close()

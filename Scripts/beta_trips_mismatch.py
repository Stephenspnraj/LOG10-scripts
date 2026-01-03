import os
import csv
import pymysql
import pandas as pd

# MySQL database connection details (direct, as in the sample script)
mysql_host = 'log-10-replica-single.cco3osxqlq4g.ap-south-1.rds.amazonaws.com'
mysql_port = 3306  # Assuming default MySQL port
mysql_user = 'meesho'
mysql_password = 'dYDxdwV*qf6rDcXiWFkCCcVH'
mysql_db = 'loadshare'

# Define the SQL query
sql_query = """
select trip_reference_number as trip_code,
       connectionCode as 3rd_part_connections,
       synced_connections,
       tbags as 3rds_party_bags,
       synced_bags,
       consignment as 3rd_part_shipments,
       synced_shipments
from 
(
SELECT 
    trip_reference_number,
    count(distinct JSON_UNQUOTE(JSON_EXTRACT(request, '$.connectionCode'))) AS connectionCode,
    count(distinct bags.bag_number) as tbags,
    count(distinct JSON_UNQUOTE(JSON_EXTRACT(consignments.consignments, '$'))) AS consignment
FROM 
    third_party_trips_request,
    JSON_TABLE(
        JSON_EXTRACT(request, '$.waybillNumbers.bags[*]'),
        "$[*]" COLUMNS (
            bag_number VARCHAR(255) PATH '$.bag_number',
            bag_destination VARCHAR(255) PATH '$.bag_destination',
            consignment_type VARCHAR(255) PATH '$.consignment_type',
            consignments JSON PATH '$.consignments'
        )
    ) AS bags
LEFT JOIN JSON_TABLE(
        bags.consignments,
        "$[*]" COLUMNS (
            consignments JSON PATH '$'
        )
    ) AS consignments ON true
WHERE 
    trip_reference_number = %s
group by 1 
)y
cross join 
(
select t.code,
    count(distinct c.id) AS synced_connections,
    count(distinct m.id) as synced_bags,
    count(distinct cg.waybill_no) AS synced_shipments
from trips t 
left join connections c
    on t.id = c.trip_id
left join entity_consignment_mapping ecm
    on c.id = ecm.destination_entity_id
left join manifests m
    on ecm.source_entity_id = m.id
left join consignment_groups cg
    on m.id = cg.entity_id
where t.code = %s
group by 1
)x ;
"""

# Input and output file paths
input_csv = 'input.csv'
output_file = 'output.xlsx'

# Delete output file if it exists
if os.path.exists(output_file):
    os.remove(output_file)

# Append timestamp to output file name
output_file_timestamped = f'output.xlsx'

# Connect to MySQL directly (without SSH tunnel)
conn = pymysql.connect(
    host=mysql_host,
    port=mysql_port,
    user=mysql_user,
    password=mysql_password,
    db=mysql_db,
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor
)

# Initialize lists for storing mismatch data
all_mismatches = []
connection_mismatches = []
manifest_mismatches = []
shipment_mismatches = []

# Open the input CSV file and read trip_reference_numbers
with open(input_csv, 'r') as csv_file:
    csv_reader = csv.reader(csv_file)
    next(csv_reader)  # Skip header row
    for row in csv_reader:
        trip_reference_number = row[0]

        # Execute the SQL query for the current trip_reference_number
        with conn.cursor() as cursor:
            cursor.execute(sql_query, (trip_reference_number, trip_reference_number))
            query_result = cursor.fetchall()

            for result in query_result:
                # Check for mismatches
                connections_mismatch = result['3rd_part_connections'] != result['synced_connections']
                bags_mismatch = result['3rds_party_bags'] != result['synced_bags']
                shipments_mismatch = result['3rd_part_shipments'] != result['synced_shipments']

                # Store in all mismatches
                if connections_mismatch or bags_mismatch or shipments_mismatch:
                    all_mismatches.append(result)

                # Store in respective mismatch categories
                if connections_mismatch:
                    connection_mismatches.append(result)
                if bags_mismatch:
                    manifest_mismatches.append(result)
                if shipments_mismatch:
                    shipment_mismatches.append(result)

# Write the results to an Excel file
with pd.ExcelWriter(output_file_timestamped, engine='xlsxwriter') as writer:
    # Create DataFrames for each sheet
    if all_mismatches:
        df_all_mismatches = pd.DataFrame(all_mismatches)
        df_all_mismatches.to_excel(writer, sheet_name='all_mismatches', index=False)
    if connection_mismatches:
        df_connection_mismatches = pd.DataFrame(connection_mismatches)
        df_connection_mismatches.to_excel(writer, sheet_name='connection_mismatches', index=False)
    if manifest_mismatches:
        df_manifest_mismatches = pd.DataFrame(manifest_mismatches)
        df_manifest_mismatches.to_excel(writer, sheet_name='manifest_mismatches', index=False)
    if shipment_mismatches:
        df_shipment_mismatches = pd.DataFrame(shipment_mismatches)
        df_shipment_mismatches.to_excel(writer, sheet_name='shipment_mismatches', index=False)

print(f"Output Excel file created: {output_file_timestamped}")

# Close MySQL connection
conn.close()

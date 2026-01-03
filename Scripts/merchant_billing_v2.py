import os
import csv
import pymysql
from datetime import datetime
# SSH tunnel configuration
mysql_host = 'log-10-replica-single.cco3osxqlq4g.ap-south-1.rds.amazonaws.com'
mysql_port = 3306  # Assuming default MySQL port
mysql_user = 'meesho'
mysql_password = 'dYDxdwV*qf6rDcXiWFkCCcVH'
mysql_db = 'loadshare'

# Define the SQL query
sql_query = """
  select
  m.flow_type,
  cg.waybill_no,
  c.order_ref_no,
  c.consignment_status,
  c.payment_type,
  m.manifest_code,
  ecm.waybill_no as docket_no,
  cs.created_at as inscan_at,
  des.alias as detination_location,
  pick_c.name as pickup_city,
  pick_c.state as pickup_state,
  pick_p.zipcode as pickup_pincode,
  drop_c.name as drop_city,
  drop_c.state as drop_state,
  drop_p.zipcode as drop_pincode,
  c.consignment_amount AS declared_value
from
  connections con
  join locations des on con.destination_loc_id = des.id
  join entity_consignment_mapping ecm on con.id = ecm.destination_entity_id
  join manifests m on m.id = ecm.source_entity_id
  join consignment_groups cg on cg.entity_id = m.id
  join consignments c on c.waybill_no = cg.waybill_no
  and c.partner_id in (268, 269)
  left join consignment_scans cs on cs.waybill_no = c.waybill_no
  and cs.location_id = m.originated_loc_id
  and (
    (
      cs.scan_type = 'RTO_IN'
      and m.flow_type = 'RTO'
    )
    OR (
      cs.scan_type = 'IN_SCAN'
      and m.flow_type = 'FORWARD'
    )
  )
  join locations pick_l on c.customer_pickup_loc_id = pick_l.id
  join cities pick_c on pick_c.id = pick_l.city_id
  join pincodes pick_p on pick_p.id = pick_l.pincode_id
  join pincodes drop_p on drop_p.id = c.pincode_id
  join cities drop_c on drop_c.id = drop_p.city_id
where
  con.connection_code = % s
group by
  1,
  2,
  7;
"""

# Input and output files
input_csv = 'input.csv'
output_file = 'output.csv'

# Delete output file if it exists
if os.path.exists(output_file):
    os.remove(output_file)

# Append timestamp to output file name
#output_file_timestamped = f'output_{datetime.now().strftime("%Y%m%d%H%M%S")}.csv'
output_file_timestamped='output.csv'
# Create SSH tunnel
conn = pymysql.connect(
            host=mysql_host,
            port=mysql_port,
            user=mysql_user,
            password=mysql_password,
            db=mysql_db,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
)
    # Open the input CSV file and read trip_reference_numbers
with open(input_csv, 'r') as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader)  # Skip header row
        with open(output_file_timestamped, 'w') as outfile:
            writer = csv.writer(outfile)
            first_trip = True  # Flag to indicate if it's the first trip
            total_trips = sum(1 for _ in csv_reader)  # Count total number of trips
            csv_file.seek(0)  # Reset file pointer to start
            next(csv_reader)  # Skip header row
            for index, row in enumerate(csv_reader, 1):
                trip_reference_number = row[0]

                # Execute the SQL query for the current trip_reference_number
                with conn.cursor() as cursor:
                    cursor.execute(sql_query, (trip_reference_number,))
                    query_result = cursor.fetchall()

                    if query_result:
                        if first_trip:
                            # Write the column headers to the output file only for the first trip
                            writer.writerow(query_result[0].keys())
                            first_trip = False

                        # Write the query result to the output file
                        for result in query_result:
                            writer.writerow(result.values())

                # Print progress
                print(f"Processed trip {index}/{total_trips}")
                print(f"Progress: {index/total_trips * 100:.2f}%")
                print()

    # Close MySQL connection
    
conn.close()
print(f"Output file created: {output_file_timestamped}")

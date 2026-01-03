import inspect
from sqlalchemy import create_engine, text
import pandas as pd
import sys
from logging import getLogger
import os
from os import path

# Add the parent directory to the path the be able to import the utils folder
current_dir = path.dirname(path.abspath(inspect.getfile(inspect.currentframe())))
sys.path.insert(0, path.dirname(current_dir))

logger = getLogger()

PROD_DB_CREDS = {
    "user": "log10_scripts",
    "password": "D2lx7Wz0Wm9ISAY-Vp1gxKDTzLRRl5k1m",
    "host": "log10-replica-replica.cco3osxqlq4g.ap-south-1.rds.amazonaws.com",
    "port": "3306",
    "dbname": "loadshare"
}

REDSHIFT_DB_CREDS = {
    "user": "redshift_admin",
    "password": "kI^&(ShPO8w7wew_s9",
    "host": "17.0.1.80",
    "port": "5439",
    "dbname": "dev"
}

prod_db_url = f"mysql+mysqlconnector://{PROD_DB_CREDS['user']}:{PROD_DB_CREDS['password']}@{PROD_DB_CREDS['host']}:{PROD_DB_CREDS['port']}/{PROD_DB_CREDS['dbname']}"
redshift_db_url = f"postgresql+psycopg2://{REDSHIFT_DB_CREDS['user']}:{REDSHIFT_DB_CREDS['password']}@{REDSHIFT_DB_CREDS['host']}:{REDSHIFT_DB_CREDS['port']}/{REDSHIFT_DB_CREDS['dbname']}"

prod_engine = create_engine(prod_db_url)
redshift_engine = create_engine(redshift_db_url)

#Replace this with automator
old_loc_ip = int(os.environ['old_loc_id'])
new_loc_ip = int(os.environ['new_loc_id'])
shipper_id_ip = int(os.environ['shipper_id'])
loc_type_ip = os.environ['loc_type']
is_dry_run = int(os.environ['is_dry_run'])


def main():

    if is_dry_run > 1:
        print("Test")
        return
    
    print(f"({shipper_id_ip},{old_loc_ip},{new_loc_ip},'{loc_type_ip}','{is_dry_run}');")
    
    if old_loc_ip and new_loc_ip and shipper_id_ip and loc_type_ip and is_dry_run is not None:
        insert_config_query = (f"INSERT INTO path_correction_config(pincode_id,old_loc,new_loc,loc_type,is_dry_run) VALUES "
                               f"({shipper_id_ip},{old_loc_ip},{new_loc_ip},'{loc_type_ip}',{is_dry_run});")
        try:
            with prod_engine.connect() as conn:
                conn.execute(insert_config_query)
                conn.commit()
            print("Insert query executed successfully")
        except Exception as e:
            print(f"Error during insertion: {e}")

    if is_dry_run == 1:
        cofig_query = """
                    select * from path_correction_config
                    where status = 1
                    and loc_type = 'SHIPPER'
                    and is_dry_run = 1;
                    """
    else:
        cofig_query = """
                    select * from path_correction_config
                    where status = 1
                    and loc_type = 'SHIPPER';
                    """
        
    df = pd.read_sql_query(cofig_query, prod_engine)

    print(df)

    for index, row in df.iterrows():
        shipper_id = row['pincode_id']
        old_loc = row['old_loc']
        new_loc = row['new_loc']
        row_id = row['id']

        print(f"{row_id} - Started from Shipper {shipper_id} from {old_loc} to {new_loc}")

        id_list = []
        waybill_list = []

        main_query = f"""select cep.id as id, cep.waybill_no as waybill_no
                        from raw_data.consignments c
                        join raw_data.consignment_expected_path cep on cep.waybill_no = c.waybill_no
                        and cep.location_id = {old_loc}
                        and cep.flow_type = '__flow_type__'
                        where shipper_id = {shipper_id}
                        and c.consignment_status not in ('HANDOVER','BOOKING_CANCELLED','DEL','RTO_HANDOVER','RTODEL','SL')
                        and c.created_at > current_date - interval '45 days'
                        and c.flow_type = '__flow_type__'
                        and c.location_id <> {old_loc}
                        and cep.is_client_path = 1
                        group by 1,2;""" 
        # Print query and row count
        
        copy_query = main_query
        
        rto_check_df = pd.read_sql_query(copy_query.replace("__flow_type__","RTO"), redshift_engine)

        if rto_check_df.empty:
            print("RTO List is Empty")
            copy_query = main_query
            fwd_check_df = pd.read_sql_query(copy_query.replace("__flow_type__","FORWARD"), redshift_engine)
            if fwd_check_df.empty:
                print("FWD List is also Empty")
                status_correction = f"update path_correction_config set status = 0 where id = {row_id};"
                print("Setting config status to 0")
                with prod_engine.connect() as conn:
                    conn.execute(status_correction)
        else:
            id_list.extend(rto_check_df['id'])
            waybill_list.extend(rto_check_df['waybill_no'])
            
        list_length = len(id_list) 


        for i in range(0, len(id_list), 10):
            batch = id_list[i:i+10]
            
            update_cep_query = f"UPDATE consignment_expected_path SET location_id = {new_loc} WHERE id IN ({','.join(map(str, batch))})"

            if is_dry_run == 1:
                print(f"Number of rows to be affected : {list_length}")
                print(f"Waybills to be affected : {waybill_list}")
                config_correction = "UPDATE path_correction_config SET status = 0 WHERE is_dry_run = 1"
                with prod_engine.connect() as conn:
                    conn.execute(config_correction)
            else:
                print(f"Number of rows to be affected : {list_length}")
                print(f"IDs Affected : {id_list}")
                with prod_engine.connect() as conn:
                    conn.execute(update_cep_query)
        
        print(f"Done for Pincode ID : {shipper_id}")

if __name__ == "__main__":
    main()

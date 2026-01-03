import pandas as pd
from utils import DBManager, Config
from datetime import datetime
from os import environ

SortCentre_alias = ['LDNS','BLRS','KLPS','HBLS','HYDS','CIS','CYS','IMS','ZPS','NKS','NNS','HTS','GVS','LAS','PYS','RRS','RWS','EPS','DHS','BUS','KOS','DBS','GHS','BHS','LUS', 'PES', 'RPS', 'NUS','SYS','JBS','BBS','BLS','KYS','INS','HYS','BWS']
#SortCentre_alias = ['LDNS']

configObj = Config()
db = DBManager(configObj.hydra_prime_credentials)

for SC_alias in SortCentre_alias: 
    query = f"""SELECT
                    l.id AS location_id, cl.id AS next_location_id
                FROM
                    network_metadata nm
                JOIN locations l ON
                    nm.next_location_alias = l.alias
                JOIN locations cl ON
                    cl.alias = '{SC_alias}'
                WHERE
                    nm.location_alias = CONCAT('{SC_alias}', '.LMSC')
                    AND nm.is_active = 1
                    and  l.location_ops_type<>'PUDO'
                    AND l.entity_id NOT IN ('127788', '127798', '127869', '128146')"""

    log10_query = db.fetchData(query).fetchall()

    # If no data is returned, skip to the next alias
    if not log10_query:
        print(f"No data found for {SC_alias}, skipping...")
        continue  

    # Convert result to DataFrame
    df = pd.DataFrame(log10_query, columns=["location_id", "next_location_id"])
    
    locations = list(df["location_id"])
    next_locations = df["next_location_id"].iloc[0]  # Ensure it's accessed safely
    print(locations)
    print(next_locations)

    def Misroute_configs(from_loc, to_loc):
        Query = f"""SELECT id, entity_id FROM locations WHERE id = {from_loc}"""
        log10_Consignments1 = db.fetchData(Query).fetchall()

        if not log10_Consignments1:
            print(f"No data found for from_loc: {from_loc}")
            return

        print(log10_Consignments1[0]['id'], log10_Consignments1[0]['entity_id'])

        Query = f"""SELECT id, entity_id, pincode_id FROM locations WHERE id = {to_loc}"""
        log10_Consignments2 = db.fetchData(Query).fetchall()

        if not log10_Consignments2:
            print(f"No data found for to_loc: {to_loc}")
            return

        print(log10_Consignments2[0]['id'], log10_Consignments2[0]['entity_id'], log10_Consignments2[0]['pincode_id'])

        Query = f"""SELECT id FROM next_location_configs 
                    WHERE location_id = {log10_Consignments1[0]['id']} 
                    AND next_location_id = (SELECT id FROM locations WHERE alias = '{SC_alias}') 
                    AND pincode_id = {log10_Consignments2[0]['pincode_id']} 
                    AND entity_type = 'MANIFEST' AND is_active = 1"""
        log10_insert_nlc_s = db.fetchData(Query).fetchall()

        if not log10_insert_nlc_s:
            Query_inst1 = f"""INSERT INTO loadshare.next_location_configs 
                              (customer_id, location_id, next_location_id, pincode_id, return_available, 
                              entity_type, is_active, is_manual, audit_log) 
                              VALUES (10823, {log10_Consignments1[0]['id']}, 
                              (SELECT id FROM locations WHERE alias = '{SC_alias}'), 
                              {log10_Consignments2[0]['pincode_id']}, 1, 'MANIFEST', 1, 1, 'WF_BAGGING_CRON');"""
            
            db.fetchData(Query_inst1)
            
            Query = f"""INSERT INTO loadshare.db_update_logs 
                        (actor_name, db_query, application_name) 
                        VALUES ('B Shanmugamani', "{Query_inst1}", 'Jenkins-Automations-MisrouteConfig')"""
            
            print(Query)
            db.fetchData(Query)

    for i in locations:
        for j in locations:
            if i != j:
                Misroute_configs(i, j)

    print('Misroute Configs Mapping updated')

db.close()

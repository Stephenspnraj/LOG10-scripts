import pandas as pd
from utils import DBManager, Config
from datetime import datetime
from os import environ

# SortCentre_alias = ['BHS','LUS']

# SortCentre_alias = ['BUS','KOS','DBS','GHS','BHS','LUS']


# for SC_alias in SortCentre_alias: 

def Vendor_Mapping(SC_alias,Vendor_name):
    
    if len(Vendor_name.split(","))!=0:
      if len(Vendor_name.split(","))==1:
            Vendors = list(Vendor_name.split(","))
            print(Vendors)
      elif len(Vendor_name.split(","))>=2:
            Vendors = list(Vendor_name.split(","))
            print(Vendors)



    for Vendor in Vendors:
                        query_01 = f"""select id from vendors where partner_id =(select entity_id from locations where alias = '{SC_alias}') and name ='{Vendor}'"""

                        configObj = Config()
                        db = DBManager(configObj.hydra_prime_credentials)
                        log10_insert_v =  db.fetchData(query_01).fetchall()
                        db.close()
                        if len(log10_insert_v)==0:
                                        Query_i= f"""INSERT INTO loadshare.vendors  (partner_id, name, email, contact_number,status ,vendor_partner_id , is_vendor, is_coloader, is_last_mile, is_mid_mile, is_ftl, is_first_mile, is_blocked, temp_unblock_date, is_eligible_to_block)
                                        select partner_id,'{Vendor}' as name, v.email, v.contact_number,v.status , vendor_partner_id , v.is_vendor, v.is_coloader, v.is_last_mile, v.is_mid_mile, v.is_ftl, v.is_first_mile, v.is_blocked, v.temp_unblock_date, v.is_eligible_to_block from vendors v
                                        where partner_id = (select entity_id from locations where alias = '{SC_alias}') group by partner_id"""
                                        print(Query_i)

                                        configObj = Config()
                                        db = DBManager(configObj.hydra_prime_credentials)

                                        log10_query =  db.fetchData(Query_i).fetchall()
                                        db.close()

                        
                        

                        if SC_alias != 'FRS':
                                query = f"""select DISTINCT  entity_id from next_location_configs nlc
                                        join locations l
                                        on l.id = nlc.location_id
                                        and location_ops_type = 'LM'
                                        where next_location_id = (select id from locations where alias like '{SC_alias}') and nlc.is_active = 1 and flow_type = 'RTO'
                                        group by alias"""
                        
                        elif SC_alias == 'FRS':
                                query = f"""select DISTINCT  entity_id from next_location_configs nlc
                                        join locations l
                                        on l.id = nlc.location_id
                                        where next_location_id = (select id from locations where alias like '{SC_alias}') and nlc.is_active = 1 and flow_type = 'RTO'
                                        group by alias"""

                        configObj = Config()
                        db = DBManager(configObj.hydra_prime_credentials)
                        log10_query =  db.fetchData(query).fetchall()
                        db.close()


                        df = pd.DataFrame(log10_query)
                        pdf = df.copy()
                        partner_id = list(pdf["entity_id"])
                        print(partner_id)

                        for partner in partner_id:
                                Query =f"""select {partner} as partner_id, v.name, v.email, v.contact_number,v.status , {partner} as vendor_partner_id , v.is_vendor, v.is_coloader, v.is_last_mile, v.is_mid_mile, v.is_ftl, v.is_first_mile, v.is_blocked, v.temp_unblock_date, v.is_eligible_to_block from vendors v 
                                        left join vendors v2
                                        on v.name = v2.name
                                        and v2.partner_id = {partner}
                                        where v.partner_id = (select entity_id from locations where alias like '{SC_alias}')
                                        and v2.name  is null
                                        group by v.name;"""
                                configObj = Config()
                                db = DBManager(configObj.hydra_prime_credentials)   
                                log10_insert_cb_s =  db.fetchData(Query).fetchall()
                                db.close()
                                if len(log10_insert_cb_s)!=0:
                                        Query_p= f"""INSERT INTO loadshare.vendors  (partner_id, name, email, contact_number,status ,vendor_partner_id , is_vendor, is_coloader, is_last_mile, is_mid_mile, is_ftl, is_first_mile, is_blocked, temp_unblock_date, is_eligible_to_block)
                                        select {partner} as partner_id, v.name, v.email, v.contact_number,v.status , {partner} as vendor_partner_id , v.is_vendor, v.is_coloader, v.is_last_mile, v.is_mid_mile, v.is_ftl, v.is_first_mile, v.is_blocked, v.temp_unblock_date, v.is_eligible_to_block from vendors v 
                                        left join vendors v2
                                        on v.name = v2.name
                                        and v2.partner_id = {partner}
                                        where v.partner_id = (select entity_id from locations where alias like '{SC_alias}')
                                        and v2.name  is null
                                        group by v.name"""
                                        print(Query_p)

                                        configObj = Config()
                                        db = DBManager(configObj.hydra_prime_credentials)

                                        log10_query =  db.fetchData(Query_p).fetchall()
                                        db.close() 

                                
                                
                        print(SC_alias)
                        print(("*")*8)   


SC_alias = environ["Sorting_Centre"]
Vendor_name = environ["Vendor_Name"]

Vendor_Mapping(SC_alias,Vendor_name)
                        

import pandas as pd
# from utils import DBManager, Config
from utils import DBManager, Config
# from config_wrn import Config_wrm
from config_wrn import Config_wrm
from os import environ
import requests
import json
import sys
import time





Query ="""select br.waybill_no ,p.id as pincode_id from booking_requests br 

join pincodes p

on p.zipcode = cast(JSON_EXTRACT((br.waybill_data -> '$.consignee'), '$.pincode') as float)

left join consignments c on br.waybill_no = c.waybill_no where br.created_at between current_time() -interval 5 days  and current_time()   and c.waybill_no is null and json_valid(br.waybill_data) = 1  group by br.waybill_no ;"""

configObj = Config()
db = DBManager(configObj.hydra_prime_credentials)
log10_Consignments =  db.fetchData(Query).fetchall()
db.close()
df = pd.DataFrame(log10_Consignments)
df

location_id = [5880998,5881069,5879890,5879889,5879888,5879884,5879891,5880472,5881032,5880763,5880709,5881063,5881099,5880368,5881067,5881080,5881065,5881106,5879982,5879881,5881034,5881017,5880506,5880208,5881030,5880958,5881086,5879879,5879900]
next_location_id = [5881000,5881032,5881032,5881032,5881032,5881032,5881032,5881000,5881000,5881032,5881032,5881032,5881032,5881000,5881032,5881032,5881032,5881032,5881032,5881032,5881032,5881032,5881000,5881000,5881032,5881032,5881032,5881032,5881032]
wrn_location_id = [373,500,506,207,208,209,210,211,202,219,204,206,261,508,528,531,283,543,584,590,603,607,636,638,640,642,653,659,636,679,672]
wrn_next_location_id=[212,212,212,283,283,283,283,283,283,283,283,283,283,212,283,283,212,283,212,283,283,283,283,283,283,283,283,283,283,283,283]


headers = {
            'token': 'b4d0188e-feb8-4e4c-a84d-3277333f138a',
            'tokenId': '991454a2-569f-4574-b97b-949a428413d7',
            'deviceId': '123123123123',}
 
json_data = {
            'username': 'vineeth.lsn',
            'password': '12345',} 




response_login = requests.post('https://meesho-api.loadshare.net/v1/login', headers=headers, json=json_data).json()

token = response_login['response']['token']['accessToken']

tokenid = response_login['response']['token']['tokenId']



for i in range(len(df)):
    for ind in range(len(location_id)):
        Query = f"""select id from next_location_configs where location_id ={location_id[ind]} and next_location_id = {next_location_id[ind]} and pincode_id ={df['pincode_id'][i]}"""
        configObj = Config()
        db = DBManager(configObj.hydra_prime_credentials)
        log10_Consignments =  db.fetchData(Query).fetchall()
        db.close()
        if len(log10_Consignments)==0:
            Query22 = f"""INSERT INTO next_location_configs (customer_id, location_id, next_location_id, pincode_id, return_available, entity_type, is_active) VALUES(10823,{location_id[ind]} ,{next_location_id[ind]},{df['pincode_id'][i]}, 1, 'WAYBILL', 1);"""
            # print(Query22)
            configObj = Config()
            db = DBManager(configObj.hydra_prime_credentials)
            log10_Consignments =  db.runQuery(Query22).fetchall()
            db.close()
            Query23 =f"""INSERT INTO loadshare.db_update_logs (actor_name, db_query, application_name) VALUES('B Shanmugamani'," {Query22} ", 'Jenkins-Automations')"""
            configObj = Config()
            db = DBManager(configObj.hydra_prime_credentials)   
            log10_p_to_p =  db.fetchData(Query23).fetchall()
            db.close() 
            
    for ind in range(len(wrn_location_id)):
        Query = f"""select id from movement_location_configs where location_id ={wrn_location_id[ind]} and next_location_id = {wrn_next_location_id[ind]} and pincode_id ={df['pincode_id'][i]}"""
        configObj = Config_wrm()
        db = DBManager(configObj.hydra_prime_credentials)
        log10_Consignments =  db.fetchData(Query).fetchall()
        db.close()
        if len(log10_Consignments)==0:
            Query22 = f"""INSERT INTO movement_location_configs(pincode_id, location_id, next_location_id, customer_id, is_active) VALUES({df['pincode_id'][i]}, {wrn_location_id[ind]}, {wrn_next_location_id[ind]}, 10823, 1);"""
            configObj = Config_wrm()
            db = DBManager(configObj.hydra_prime_credentials)
            log10_Consignments =  db.runQuery(Query22).fetchall()
            db.close()
            Query23 =f"""INSERT INTO loadshare.db_update_logs (actor_name, db_query, application_name) VALUES('B Shanmugamani'," {Query22} ", 'Jenkins-Automations')"""
            # print(Query)
            configObj = Config()
            db = DBManager(configObj.hydra_prime_credentials)   
            log10_p_to_p =  db.fetchData(Query23).fetchall()
            db.close() 

    Waybill = df['waybill_no'][i]

    headers = {
    'token': token,
    'tokenid': tokenid,
    'Content-Type': 'application/json',}
    json_data = {
    'waybillNos': [Waybill,],}

    response = requests.post('https://api-3p.loadshare.net/tp/v1/bookings/validate_and_retry', headers=headers, json=json_data)
    print(response)

    print(i+1,")",Waybill," ",response.json())
    print("")

            

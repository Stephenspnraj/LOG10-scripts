import pandas as pd
from utils import DBManager, Config
from config_wrn import Config_wrm
from os import environ
import requests
import json
import sys
import time


Query ="""select count(*) as count_num from consignment_lost_request where status='PENDING'and is_active = 1 ;"""

configObj = Config()
db = DBManager(configObj.hydra_prime_credentials)
log10_Consignments =  db.fetchData(Query).fetchall()
db.close()
df = pd.DataFrame(log10_Consignments)

print(f"""Total Shipments to mark lost are : {df['count_num'][0]} """)


if df['count_num'][0]!=0:

    headers1 = {
            'token': 'b4d0188e-feb8-4e4c-a84d-3277333f138a',
            'tokenId': '991454a2-569f-4574-b97b-949a428413d7',
            'deviceId': '123123123123',}
 
    json_data1 = {
            'username': 'vineeth.lsn',
            'password': '12345',} 
    
    response_login1 = requests.post('https://meesho-api.loadshare.net/v1/login', headers=headers1, json=json_data1).json()


    token = response_login1['response']['token']['accessToken']

    tokenid = response_login1['response']['token']['tokenId']

    headers = {
    'token': token,
    'tokenid': tokenid,}

    response = requests.get('https://meesho-api.loadshare.net/b2b/v2/consignments/processPendingLostRequest', headers=headers)

    print(1,response)


    headers2 = {
            'token': 'b4d0188e-feb8-4e4c-a84d-3277333f138a',
            'tokenId': '991454a2-569f-4574-b97b-949a428413d7',
            'deviceId': '123123123123',}
 
    json_data2 = {
            'username': '0009368483',
            'password': '0009368483',} 
    
    response_login2 = requests.post('https://meesho-api.loadshare.net/v1/login', headers=headers2, json=json_data2).json()


    token = response_login2['response']['token']['accessToken']

    tokenid = response_login2['response']['token']['tokenId']

    headers = {
    'token': token,
    'tokenid': tokenid,}

    response = requests.get('https://meesho-api.loadshare.net/b2b/v2/consignments/processPendingLostRequest', headers=headers)

    print(2,response)

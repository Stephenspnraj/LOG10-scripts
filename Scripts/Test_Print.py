import pandas as pd
from utils import DBManager, Config
from config_wrn import Config_wrm
from os import environ
import requests
import json

lst = [127992 ,127993 ,127994 ,128000 ,128001 ,128002 ,128003 ,128006 ,128007 ,128013 ,127925]

for i in lst:
    for j in lst:
        if i!=j:
            query_01 = f"""select * from partner_to_partner_mapping where source_partner_id = {i} and link_partner_id = {j} and is_active =1"""
            configObj = Config()
            db = DBManager(configObj.hydra_prime_credentials)
            log10_insert_v =  db.fetchData(query_01).fetchall()
            db.close()
            if len(log10_insert_v)==0:
                                Query_i= f""" INSERT INTO loadshare.partner_to_partner_mapping(source_partner_id, link_partner_id, is_active, customer_id) VALUES({i}, {j}, 1, 0);"""
                                configObj = Config()
                                db = DBManager(configObj.hydra_prime_credentials)
                                log10_query =  db.fetchData(Query_i).fetchall()
                                db.close()

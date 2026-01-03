# import logging
# import os
# import requests
# import pymysql
# import json
# import datetime
# from slack import WebClient
# from slack.errors import SlackApiError

# class Config:
#     def __init__(self):
#         self.slack_token = "pT3BHK5oIVNM8xfoDOdGOpXg"
#       #  self.hydra_alerts_channel_url = "https://hooks.slack.com/services/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"
#     # def get_db(self):
#         if os.environ.get("DBINFO") == "STAGING":
#             self.hydra_prime_credentials = {
#                         "host": "log10-staging.cco3osxqlq4g.ap-south-1.rds.amazonaws.com",
#                         "user": "log10_staging",
#                         "passwd":"A_edjsHKmDF6vajhL4go6ekP",
#                         "db": "loadshare",
#                         "port": 3306
#                         # "port": 3307
#                     }
#             self.LOG_PREFIX = "hydra-prime "      
#         else :
#             self.hydra_prime_credentials = {
#                         "host": "log10-replica-replica.cco3osxqlq4g.ap-south-1.rds.amazonaws.com",
#                         "user": "log10_scripts",
#                         "passwd": "D2lx7Wz0Wm9ISAY-Vp1gxKDTzLRRl5k1m",
#                         "db": "loadshare",
#                         "port": 3306
#                     } 
#             self.LOG_PREFIX = "hydra-prime "

#     # def __init__(self):
#     #     self.slack_token = "pT3BHK5oIVNM8xfoDOdGOpXg"
#     #     self.hydra_alerts_channel_url = "https://hooks.slack.com/services/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"
#     #     self.hydra_prime_credentials = {
#     #         # "host": "log10-staging.cco3osxqlq4g.ap-south-1.rds.amazonaws.com",
            
#     #         "host": "log10-db.cco3osxqlq4g.ap-south-1.rds.amazonaws.com",
#     #         # "host": "127.0.0.1",
#     #         # "user": "log10_staging",
            
#     #         "user": "log10_scripts",
#     #         # "passwd":"A_edjsHKmDF6vajhL4go6ekP",

#     #         "passwd": "D2lx7Wz0Wm9ISAY-Vp1gxKDTzLRRl5k1m",
#     #         "db": "loadshare",
#     #         "port": 3306
#     #     }
#     #     self.LOG_PREFIX = "hydra-prime"

# SLOWQUERY_THRESHOLD = 10
# configObj = Config()

# def get_logger(name, level=logging.INFO):
#     formatter = logging.Formatter(f"%(asctime)s|{name.upper()}%(message)s", "%Y-%m-%s %H:%M:%S")
#     # handler = logging.FileHandler(f"logs/{name}.txt")
#     handler = logging.StreamHandler()
#     handler.setFormatter(formatter)
#     logger = logging.getLogger(name)
#     logger.handlers = []
#     logger.setLevel(level)
#     logger.addHandler(handler)
#     return logger

# class DBManager(object):
#     def __init__(self, dbconfig):
#         self.logger = get_logger(configObj.LOG_PREFIX)
#         self.db = pymysql.connect(host=dbconfig['host'], user=dbconfig['user'], passwd=dbconfig['passwd'], port=dbconfig['port'], db=dbconfig['db'], charset='utf8', autocommit=True)
#         self.cursor = self.db.cursor(pymysql.cursors.DictCursor)
#         self.cursorTuple = self.db.cursor()

#     def fetchData(self, sql, args=()):
#         query_started_at = datetime.datetime.now()
#         self.cursor.execute(sql, args)
#         execution_time = (datetime.datetime.now() - query_started_at).total_seconds()
#         self.logger.info(self.cursor._executed)
#         if execution_time > SLOWQUERY_THRESHOLD:
#             self.logger.info("Unexpected slowquery: %s sec: %s", str(execution_time), self.cursor._executed.replace("\n", " "))
#         return self.cursor
    
#     def fetchDataTuple(self, sql, args=()):
#         query_started_at = datetime.datetime.now()
#         self.cursorTuple.execute(sql, args)
#         execution_time = (datetime.datetime.now() - query_started_at).total_seconds()
#         self.logger.info(self.cursorTuple._executed)
#         if execution_time > SLOWQUERY_THRESHOLD:
#             self.logger.info("Unexpected slowquery: %s sec: %s", str(execution_time), self.cursor._executed.replace("\n", " "))
#         return self.cursorTuple

#     def runQuery(self, sql, args=()):
#         try:
#             query_started_at = datetime.datetime.now()
#             self.cursor.execute(sql, args)
#             execution_time = (datetime.datetime.now() - query_started_at).total_seconds()
#             print(self.cursor._executed)
#             if execution_time > SLOWQUERY_THRESHOLD:
#                 self.logger.info("Unexpected slowquery: %s sec: %s", str(execution_time), self.cursor._executed.replace("\n", " "))
#             return self.cursor
#         except Exception as err:
#             if hasattr(self.cursor, '_executed'):
#                 print("Unexpected error: SQL runQuery: Exception: ", err, ", SQL_executed: ", self.cursor._executed)
#             else:
#                 print("Unexpected error: SQL runQuery: Exception: ", err, ", SQL_sent: ", sql, ", args: ", args)
#             return None

#     def close(self):
#         self.db.close()

#     def rollback(self):
#         self.db.rollback()

#     def commit(self):
#         self.db.commit()

#     def executemany(self, insert_query, data_to_insert):
#         self.cursor.executemany(insert_query, data_to_insert)

# def post_message_to_slack(channel, user, content):
#     client = WebClient(token=configObj.slack_token)
#     try:
#         response = client.chat_postMessage(
#             channel=channel,
#             user=user,
#             text=content
#         )
#         print(response)
#     except SlackApiError as e:
#         print(e)
#         assert e.response["error"]

# def post_message_to_slack_using_hook(hook_url, content):
#     data = {
#         "Content-type": "application/json",
#         "text": content
#     }
#     try:
#         r = requests.post(url=hook_url, data=json.dumps(data))
#         print(r.text)
#     except Exception as e:
#         print("error occured while posting to slack")
#         print(e)


 
    
# # class Config_Staging:
# #     def __init__(self):
# #         self.slack_token = "pT3BHK5oIVNM8xfoDOdGOpXg"
# #         self.hydra_alerts_channel_url = "https://hooks.slack.com/services/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"
# #         # Parse credentials
# #         self.hydra_prime_credentials = {
# #             "host": "log10-staging.cco3osxqlq4g.ap-south-1.rds.amazonaws.com",
# #             "user": "log10_staging",
# #             "passwd":"A_edjsHKmDF6vajhL4go6ekP",
# #             "db": "loadshare",
# #             "port": 3306
# #             # "port": 3307
# #         }
# #         self.LOG_PREFIX = "hydra-prime"

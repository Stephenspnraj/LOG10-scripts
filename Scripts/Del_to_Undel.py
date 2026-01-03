import pandas as pd
import json
import requests
from utils import DBManager, Config
from os import environ


def Del_to_Undel(waybill_no):
      

      configObj = Config()
      db = DBManager(configObj.hydra_prime_credentials)

      if len(waybill_no.split())==1:
            waybill_no = waybill_no.split()[0]
      elif len(waybill_no.split())>=2:
            waybill_no = tuple(waybill_no.split())
      else:
            print("Not a valid input")
      if not type(waybill_no) in (str,tuple):
            raise TypeError("""Only Consignments as text are allowed as Sample Input: MS123654789 """)
      else:
            if type(waybill_no) == str :
    
                Query = f"""SELECT distinct waybill_no FROM consignments c 

                            WHERE consignment_status ='DEL' and c.waybill_no in ("{waybill_no}")
                            """
            elif type(waybill_no) == tuple:

                Query = f"""SELECT distinct waybill_no FROM consignments c 

                            WHERE consignment_status ='DEL' and c.waybill_no in  {waybill_no} 
                            """
                log10_Consignments1 =  db.fetchData(Query).fetchall()
             
                df = pd.DataFrame(log10_Consignments1)
                if len(log10_Consignments1)!=0:
                     for i in range(len(df['waybill_no'])):
                        waybill_no = str(df['waybill_no'][i]) 
                        print(waybill_no)
                        
                        update_Command1 = f"""update consignments set consignment_status = "UNDEL",last_status_reason_code = "132" WHERE waybill_no = '{waybill_no}' ;"""
                        print(update_Command1)
                        # log10_Consignments_update =  db.fetchData(update_Command1).fetchall()

                        Query2 = f"""select data from consignment_tracking where waybill_number = '{waybill_no}' and event_type = "DELIVERED";"""
                        log10_Ct1 =  db.fetchData(Query2).fetchall()
                        if len (log10_Ct1) !=0:
                             update_Command2 = f"""update consignment_tracking set data =(select data from consignment_tracking where waybill_number = '{waybill_no}' and event_type = "DELIVERED" group by waybill_number),event_type = "UNDELIVERED" where waybill_number = '{waybill_no}' and event_type = "DELIVERED"""
                            #  log10_Ct2_update =  db.fetchData(update_Command2).fetchall()
                             print(update_Command2)


                        Query3 = f"""select id from consignments_pod where waybill_no = '{waybill_no}';"""
                        log10_Ct3 =  db.fetchData(Query3).fetchall()
                        if len (log10_Ct3) !=0:
                             update_Command3 = f"""update consignments_pod set shipment_status = "UNDEL", cod_amount = 0, collected_amount=if(collected_amount>=0,0,null) ,reason_id = 132,received_by = NULL, sig_img_link = NULL where waybill_no = '{waybill_no}';"""
                            #  log10_Ct3_update =  db.fetchData(update_Command3).fetchall()
                             print(update_Command3)

                        Query4 = f"""select id from partner_transaction_payable where waybill_no = '{waybill_no}';"""
                        log10_Ct4 =  db.fetchData(Query4).fetchall()
                        if len (log10_Ct4) !=0:
                             update_Command4 = f"""delete from partner_transaction_payable where waybill_no = '{waybill_no}';"""
                            #  log10_Ct4_update =  db.fetchData(update_Command4).fetchall()
                             print(update_Command4)     


                        Query5 = f"""select id from consignment_transaction_payable where waybill_no = '{waybill_no}';"""
                        log10_Ct5 =  db.fetchData(Query5).fetchall()
                        if len (log10_Ct5) !=0:
                             update_Command5 = f"""delete from consignment_transaction_payable where waybill_no = '{waybill_no}';"""
                            #  log10_Ct5_update =  db.fetchData(update_Command5).fetchall()
                             print(update_Command5)


                        

      db.close() 


# waybill_no = "TEST-111113024067"
waybill_no = environ["Please Enter Waybill Number Below"]

if __name__ =="__main__":
     Del_to_Undel(waybill_no)

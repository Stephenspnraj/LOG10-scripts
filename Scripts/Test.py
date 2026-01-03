import pandas as pd
import json
import requests
from utils import DBManager, Config
from os import environ
import os
import sys


def test_print(entity_type,entity_code,waybill_no):
    print("*"*80)
    print("Hello")
    print("*"*80)
    print(os.environ)
    # print(sys.version)


if __name__=="__main__":
    print(os.environ)
    print("*"*80)
    print(sys.version)
    print("*"*80)
    print("*"*80)
    print(os.getenv)
    print("*"*80)
    entity_type = environ["Entity_Type"]
    entity_code = environ["Entity_Code"]
    waybill_no = environ["Waybill_No"]
    test_print(entity_type,entity_code,waybill_no)
    print("*"*80)
    print("Done")
    

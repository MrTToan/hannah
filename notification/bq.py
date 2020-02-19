import argparse
import json
import requests

import grequests
import pandas_gbq
from requests.auth import HTTPBasicAuth
from google.oauth2 import service_account

credentials = credentials = service_account.Credentials.from_service_account_file(
    './pipeline/credentials.json',
)

key = '857ca2360b5b8b641aac33ef3cd4fd5a'
pwd = 'C\'y+P6f^7%r*8(P8'
set_user_attribute_bulk_api = 'https://api.getblueshift.com/api/v1/customers/bulk'

def get_customers(running_date):
    sql_good = """
        select device_id
        from `tiki-dwh.consumer_product.cs_send_list_{}`
        where _group = 1
        and device_id is not null
    """.format(running_date)

    sql_bad = """
        select device_id
        from `tiki-dwh.consumer_product.cs_send_list_{}`
        where _group = 0
        and device_id is not null
    """.format(running_date)
    good_customer = pandas_gbq.read_gbq(sql_good, project_id='tiki-dwh', credentials=credentials).values.tolist()
    bad_customer = pandas_gbq.read_gbq(sql_bad, project_id='tiki-dwh', credentials=credentials).values.tolist()
    return good_customer, bad_customer

good_customer, bad_customer = get_customers('20200211')
# good_customer.values.tolist()
print(good_customer)
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
pwd = "C'y+P6f^7%r*8(P8"
set_user_attribute_bulk_api = 'https://api.getblueshift.com/api/v1/customers/bulk'

def set_customers_segment(device_ids, customer_segments):

    assert len(device_ids) == len(customer_segments), 'length of customers_ids and customer_segments  must be equal'

    headers = {'content-type': 'application/json'}
    max_per_call = 50
    rs = []

    for i in range(0, len(device_ids), max_per_call):
        customer_id_chunk = device_ids[i:i+max_per_call]
        customer_segment_chunk = customer_segments[i:i+max_per_call]
        print(list(zip(customer_id_chunk, customer_segment_chunk)))

        data ={"customers":
                [{
                "device_ids":customer_id,
                "AB_test":customer_segment
                } for customer_id, customer_segment in zip(customer_id_chunk, customer_segment_chunk)
            ]}
        data = json.dumps(data)
        print(data)
        r = grequests.post(set_user_attribute_bulk_api
                                , auth=HTTPBasicAuth(key, pwd)
                                , data=data
                                , headers=headers)
        rs.append(r)

    
    result = grequests.map(rs, size=5)
    print(result[0].text)
# 
set_customers_segment(['08817404-0621-4396-bd6b-753f2699343e', 'f0f6b7a8-fd4b-41db-9b30-0e51f1170530'], ['tui', 'tui'])

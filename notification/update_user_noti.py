import argparse
import json

import gevent.monkey
gevent.monkey.patch_all()

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

def set_customers_segment(device_ids, customer_segments):

        assert len(device_ids) == len(customer_segments), 'length of customers_ids and customer_segments  must be equal'

        headers = {'content-type': 'application/json'}
        max_per_call = 50
        rs = []

        for i in range(0, len(device_ids), max_per_call):
            customer_id_chunk = device_ids[i:i+max_per_call]
            # print(customer_id_chunk)
            customer_segment_chunk = customer_segments[i:i+max_per_call]

            data ={"customers":
                    [{
                    "device_ids":customer_id,
                    "AB_test":customer_segment
                    } for customer_id, customer_segment in zip(customer_id_chunk, customer_segment_chunk)
                ]}
            data = json.dumps(data)
            # print(data) 
            r = grequests.post(set_user_attribute_bulk_api
                                    , auth=HTTPBasicAuth(key, pwd)
                                    , data=data
                                    , headers=headers)
            rs.append(r)

        
        result = grequests.map(rs, size=5)
        print(result)


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

    good_customer = pandas_gbq.read_gbq(sql_good, project_id='tiki-dwh', credentials=credentials)['device_id'].values.tolist()
    bad_customer = pandas_gbq.read_gbq(sql_bad, project_id='tiki-dwh', credentials=credentials)['device_id'].values.tolist()
    return good_customer, bad_customer


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='get running date')
    parser.add_argument("running_date", help="running date")
    args = parser.parse_args()
    good_customer, bad_customer = get_customers(args.running_date)
    set_customers_segment(good_customer, ['stage1_good_customer']*len(good_customer))
    set_customers_segment(bad_customer, ['stage1_bad_customer']*len(bad_customer))

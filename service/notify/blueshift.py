import requests
import grequests
from requests.auth import HTTPBasicAuth
import argparse
from datetime import datetime, timezone
import json
# import clv_prediction.config as cfg
import time

class blueshift(object):
    def __init__(self, key, pwd, segment_id=None, template_id=None, reported_email=None, log_file=None):
        self.one_time_campaign_api = 'https://api.getblueshift.com/api/v1/campaigns/one_time'
        self.test_push_api = 'https://api.getblueshift.com/api/v1/push_templates/test_push.json'
        self.set_user_attribute = 'https://api.getblueshift.com/api/v1/customers'
        self.set_user_attribute_bulk_api = 'https://api.getblueshift.com/api/v1/customers/bulk'
        self.segment_id = segment_id
        self.template_id = template_id
        self.reported_email = reported_email
        self.key = key
        self.pwd = pwd
        self.log_file = open(log_file, 'w') if log_file != None else None

    def create_and_launch_one_time_campaign(self, campaign_name, segment_id, template_id, utm_source, utm_campaign, utm_medium, summary_emails):
        headers = {'content-type': 'application/json'}
        data = {
                'name':campaign_name,
                'startdate':datetime.now(timezone.utc).isoformat(),
                'segment_uuid':segment_id,
                'send_summary_emails':summary_emails,
                'bypass_message_limits':True,
                'send_to_unsubscribed':False,
                'triggers': [{
                    'template_uuid':template_id,
                    'utm_source':utm_source,
                    'utm_campaign':utm_campaign,
                    'utm_medium':utm_medium
                    }]
                }
        data = json.dumps(data)
        r = requests.post(self.one_time_campaign_api, auth=HTTPBasicAuth(self.key, self.pwd), data=data, headers=headers)

        return r

    def set_customer_segment(self, customer_id, customer_segment):
        headers = {'content-type': 'application/json'}
        data = {
                'customer_id':customer_id,                
                'buy_segment':customer_segment
                }
        data = json.dumps(data)
        r = requests.post(self.set_user_attribute, auth=HTTPBasicAuth(self.key, self.pwd), data=data, headers=headers)

        return r
    
    def set_customers_segment(self, customer_ids, customer_segments):

        assert len(customer_ids) == len(customer_segments), 'length of customers_ids and customer_segments  must be equal'

        headers = {'content-type': 'application/json'}
        max_per_call = 50
        rs = []

        for i in range(0, len(customer_ids), max_per_call):
            customer_id_chunk = customer_ids[i:i+max_per_call]
            customer_segment_chunk = customer_segments[i:i+max_per_call]

            data ={"customers":
                    [{
                    "customer_id":customer_id,
                    "buy_segment":customer_segment
                    } for customer_id, customer_segment in zip(customer_id_chunk, customer_segment_chunk)
                ]}
            data = json.dumps(data)
            r = grequests.post(self.set_user_attribute_bulk_api, auth=HTTPBasicAuth(self.key, self.pwd), data=data, headers=headers)
            rs.append(r)

        
        result = grequests.map(rs, size=5)
        return result

    def send_test(self, template_id, email):
        headers = {'content-type': 'application/json'}
        data = {
                'uuid': template_id,
                'email': email
                }
        data = json.dumps(data)
        r = requests.post(self.test_push_api, auth=HTTPBasicAuth(self.key, self.pwd), data=data, headers=headers)

        return r

    def push_notification(self, customer_ids, campaign_name, utm_source, utm_campaign, utm_medium, verbose=True):
        if verbose:
            print('set customer active')

        r1 = self.set_customers(customer_ids)
        
        if verbose:
            print('execute campaign with {}'.format(campaign_name))

        r2 = self.create_and_launch_one_time_campaign(campaign_name, 
                self.segment_id, 
                self.template_id, 
                utm_source, utm_campaign, utm_medium, 
                self.reported_email)

        return r1, r2

    def set_customers(self, customer_ids):
        r = self.set_customers_segment(customer_ids, ['active']*len(customer_ids))
        self.log_file.write('\n'.join(map(str, customer_ids))+'\n')

        return r

    def close_log(self):
        self.log_file.close()

    def clean_customers(self, customer_ids):
        r = self.set_customers_segment(customer_ids, ['']*len(customer_ids))

        return r

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--template_id", help="template id", required=False, default='b2395498-5282-4958-b49a-a667b83aed39')
    parser.add_argument("--email", help="customer email", required=False, default='toan.vo2@tiki.')
    args = parser.parse_args()

    blueshift_notify = blueshift('857ca2360b5b8b641aac33ef3cd4fd5a'
                                , 'C\'y+P6f^7%r*8(P8'
                                , '86e8443b-9bb9-4678-8a7a-6911bc0bcfb3'
                                , 'b2395498-5282-4958-b49a-a667b83aed39'
                                , 'toan.vo2@tiki.vn'
                                , 'log')
#    print('send notification test to {}'.format(args.email))
#    r = blueshift_notify.send_test(args.template_id, args.email)
#    print(r.text)
#    
#    start = time.time()
#    r = blueshift_notify.set_customers_segment([6855634], ['test3'])
#    print('elapsed time: {}'.format(time.time() - start))
#    print(r)

#    r = blueshift_notify.create_and_launch_one_time_campaign(
#            'data_team_test_push_1', 
#            '86e8443b-9bb9-4678-8a7a-6911bc0bcfb3',
#            'b2395498-5282-4958-b49a-a667b83aed39',
#            'data_test',
#            'data_test',
#            'data_test',
#            'quoc.pham@tiki.vn')
    rs, r2 = blueshift_notify.push_notification([8207195], 'data_team_test_push_10', 'data_test', 'data_test', 'data_test')
    for r in rs:
        print(r.text)
    print(r2.text)
    blueshift_notify.close_log()

if __name__ == '__main__':
    main()

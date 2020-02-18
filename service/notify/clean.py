from blueshift import blueshift
import clv_prediction.config as cfg
import argparse

blueshift_notify = blueshift(cfg.blueshift_key, cfg.blueshift_pwd)

def clean(log_file):
    customer_ids = open(log_file).readlines()
    customer_ids = [int(line) for line in customer_ids]
    rs = blueshift_notify.clean_customers(customer_ids)
    for r in rs:
        print(r.text)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--log", help="customer ids", required=True)
    args = parser.parse_args()
    clean(args.log)

if __name__ == '__main__':
    main()

from process_pipeline import process_pl
import pandas_gbq
import pickle
import argparse

parser = argparse.ArgumentParser(description='get running date')
parser.add_argument("running_date", help="running date")
args = parser.parse_args()

def predict(_day=args.running_date):
    sql = """
                select c.* except(user_label, vendor_id, operating_system, min_device_date)
                        # , if(date_diff_login>=7, 1, 0) user_label
                from `tiki-dwh.consumer_product.cs_user_summary_{}` c 
                left join `tiki-dwh.consumer_product.cs_user_label` a on c.deviceID = a.deviceID
    """.format(_day)
    predicted_set = pandas_gbq.read_gbq(sql, project_id = 'tiki-dwh')
    processed_predicted_set = process_pl.fit_transform(predicted_set)
    with open('customer_segmentation_model.sav', 'rb') as f:
        model = pickle.load(f)

    result = model.predict(processed_predicted_set)
    prob_class1 = model.predict_proba(processed_predicted_set)[:, 1]
    predicted_set['prediction'] = result
    predicted_set['prob_class1'] = prob_class1
    predicted_set.reset_index(inplace=True)

    table = 'consumer_product.cs_prediction_{}'.format(_day)
    pandas_gbq.to_gbq(predicted_set, table, project_id = 'tiki-dwh', if_exists='replace')

if __name__ == "__main__":
    predict()
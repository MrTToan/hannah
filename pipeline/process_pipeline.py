from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.pipeline import FeatureUnion, Pipeline
from  sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
from xgboost import XGBClassifier
from scipy import stats
import numpy as np
import pandas as pd
import pandas_gbq
import os
    
def get_data(file_name):
    filename = os.path.join(os.getcwd(),'pipeline/{}'.format(file_name))
    with open(filename, 'r') as f:
        df = pd.read_csv(f) 
    return df

def split_data(dataset):
    X_train, X_test, y_train, y_test = train_test_split(
                                                            dataset.drop(columns=['user_label'])
                                                            , dataset['user_label']
                                                            , test_size=0.2
                                                            , random_state=99)
    return X_train, X_test, y_train, y_test

class ConvertCategorical(BaseEstimator, TransformerMixin):
    def fit(self, X, y=None):
        return self 

    def transform(self, X, y=None):
        # processed_df = X
        X['type_'] = LabelEncoder().fit_transform(X['type_'])

        for column in X.columns:
            if X[column].dtypes == 'object' and column not in ('deviceID', 'type_'):
                X[column] = X[column].astype(float)
        return X

class HandlingMissingValue(BaseEstimator, TransformerMixin):
    def fit(self, X, y=None):
        return self 

    def handling_missing_value(self, X, y=None):
        X.drop(columns=['reward', 'fixed_price', 'freegift', 'phieu_dat_coc_cate'], inplace=True)
        X.fillna(0, inplace=True)
        return X
    
    def transform(self, X, y=None):
        return self.handling_missing_value(X)

class HandlingOutliers(BaseEstimator, TransformerMixin):
    def fit(self, X, y=None):
        return self 

    def handling_outliers(self, X, y=None):
        num_col = X.columns[X.dtypes != 'object']
        outlier = pd.DataFrame()
        for col in num_col:
            outlier[col] = np.abs(stats.zscore(X[col]))

        X.drop(index=outlier[outlier.values > 3].index, inplace=True)
        X.set_index('deviceID', inplace=True)

        return X
    
    def transform(self, X, y=None):
        return self.handling_outliers(X)

class NewFeatures(BaseEstimator, TransformerMixin):
    def fit(self, X, y=None):
        return self

    def transform(self, X, y=None):
        X['TT_ratio'] = X['check_s'] / X['sum_cate']
        X.fillna(0, inplace=True)
        return X


process_pl = Pipeline(memory=None, steps=[
    ('convert_categorical', ConvertCategorical()),
    ('handling_missing_value', HandlingMissingValue()),
    ('handling_outliers', HandlingOutliers()),
    ('add_new_features', NewFeatures()),
    # ('split_data', SplitData),
], verbose=True)


raw = get_data('train.csv')
processing_data = process_pl.fit(raw)
X_train, X_test, y_train, y_test = split_data(raw)
model = XGBClassifier(max_depth=9, n_estimators=100)
model.fit(X_train, y_train)
print(model.score(X_test, y_test))


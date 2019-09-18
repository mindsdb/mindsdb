from mindsdb import Predictor
import pandas as pd
from sklearn import tree


class CustomDTModel():
    def __init__(self):
        self.clf = tree.DecisionTreeClassifier()

    def set_transaction(self, transaction):
        self.transaction = transaction
        self.output_columns = self.transaction.lmd['predict_columns']
        self.input_columns = [x for x in self.transaction.lmd['columns'] if x not in self.output_columns]
        self.train_df = self.transaction.input_data.train_df
        self.test_dt = train_df = self.transaction.input_data.test_df

    def train(self):
        self.clf = clf.fit(self.train_df[self.input_columns], self.train_df[self.output_columns])

    def predict(self, mode='predict', ignore_columns=[]):
        pd.dataFrame()
        pass


predictor = Predictor(name='custom_model_test_predictor')

dt_model = CustomDTModel()

predictor.learn(to_predict='rental_price',from_data="https://s3.eu-west-2.amazonaws.com/mindsdb-example-data/home_rentals.csv",backend=dt_model)

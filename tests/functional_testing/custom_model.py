from mindsdb import Predictor
import pandas as pd


class CustomDTModel():
    def __init__(self):
        pass

    def set_transaction(self, transaction):
        self.transaction = transaction
        self.predict_columns = self.transaction.lmd['predict_columns']
        self.train_df = self.transaction.input_data.train_df
        self.test_dt = train_df = self.transaction.input_data.test_df

    def train(self):
        model.fit(train_df, train_df[predict_columns])
        pass

    def predict(self, mode='predict', ignore_columns=[]):
        pd.dataFrame()
        pass


predictor = Predictor(name='custom_model_test_predictor')

dt_model = CustomDTModel()

predictor.learn(to_predict='rental_price',from_data="https://s3.eu-west-2.amazonaws.com/mindsdb-example-data/home_rentals.csv",backend=dt_model)

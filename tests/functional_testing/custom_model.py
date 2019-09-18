from mindsdb import Predictor
import pandas as pd
from sklearn import tree
from sklearn import preprocessing


class CustomDTModel():
    def __init__(self):
        self.clf = tree.DecisionTreeClassifier()
        le = preprocessing.LabelEncoder()

    def set_transaction(self, transaction):
        self.transaction = transaction
        self.output_columns = self.transaction.lmd['predict_columns']
        self.input_columns = [x for x in self.transaction.lmd['columns'] if x not in self.output_columns]
        self.train_df = self.transaction.input_data.train_df
        self.test_dt = train_df = self.transaction.input_data.test_df


    def train(self):
        self.le_arr = {}
        for col in [*self.output_columns, *self.input_columns]:
            self.le_arr[col] = preprocessing.LabelEncoder()
            self.le_arr[col].fit(self.transaction.input_data.data_frame[col])

        X = []
        for col in self.input_columns:
            X.append(self.le_arr[col].transform(self.transaction.input_data.train_df[col]))

        Y = []
        for col in self.output_columns:
            Y.append(self.le_arr[col].transform(self.transaction.input_data.train_df[col]))

        self.clf.fit(X, Y)

    def predict(self, mode='predict', ignore_columns=[]):
        pd.dataFrame()
        pass


predictor = Predictor(name='custom_model_test_predictor')

dt_model = CustomDTModel()

predictor.learn(to_predict='rental_price',from_data="https://s3.eu-west-2.amazonaws.com/mindsdb-example-data/home_rentals.csv",backend=dt_model)

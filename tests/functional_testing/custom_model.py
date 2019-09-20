from mindsdb import Predictor
import pandas as pd
import numpy as np
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

        X = np.swapaxes(X,1,0)

        # Only works with one output column
        Y = self.le_arr[self.output_columns[0]].transform(self.transaction.input_data.train_df[self.output_columns[0]])

        self.clf.fit(X, Y)

    def predict(self, mode='predict', ignore_columns=[]):
        if mode == 'predict':
            df = self.transaction.input_data.data_frame
        if mode == 'validate':
            df = self.transaction.input_data.validation_df
        elif mode == 'test':
            df = self.transaction.input_data.test_df

        X = []
        for col in self.input_columns:
            X.append(self.le_arr[col].transform(df[col]))

        X = np.swapaxes(X,1,0)

        predictions = self.clf.predict(X)

        formated_predictions = {self.output_columns[0]: predictions}

        return formated_predictions


predictor = Predictor(name='custom_model_test_predictor')

dt_model = CustomDTModel()

predictor.learn(to_predict='rental_price',from_data="https://s3.eu-west-2.amazonaws.com/mindsdb-example-data/home_rentals.csv", backend=dt_model)
predictions = predictor.predict(when_data="https://s3.eu-west-2.amazonaws.com/mindsdb-example-data/home_rentals.csv", backend=dt_model)
print(predictions[25])

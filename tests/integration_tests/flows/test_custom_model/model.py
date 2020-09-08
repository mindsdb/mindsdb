
from sklearn.linear_model import LinearRegression
import numpy as np
import pandas as pd


class Model():
    def setup(self):
        self.model = LinearRegression()

    def get_x(self, data):
        initial_price = np.array(data['initial_price'])
        initial_price = initial_price.reshape(-1, 1)
        print(initial_price)
        return initial_price

    def get_y(self, data, to_predict_str):
        to_predict = np.array(data[to_predict_str])
        return to_predict

    def predict(self, from_data, kwargs):
        initial_price = self.get_x(from_data)
        rental_price = self.model.predict(initial_price)
        return pd.DataFrame({'rental_price': rental_price})

    def fit(self, from_data, to_predict, data_analysis, kwargs):
        Y = self.get_y(from_data, to_predict)
        X = self.get_x(from_data)
        self.model.fit(X, Y)

                     
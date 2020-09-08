
from sklearn.linear_model import LinearRegression
import numpy as np
import pandas as pd


class Model():
    def setup(self):
        self.model = LinearRegression()

    def get_x(data):
        initial_price = np.array(from_data['initial_price'])
        initial_price.reshpae(-1, 1)
        return initial_price

    def get_y(data, to_predict_str):
        to_predict = np.array(from_data[to_predict])
        return to_predict

    def predict(self, from_data, kwargs):
        initial_price = get_x(from_data)
        rental_price = self.model.predict(initial_price)
        return pd.DataFrame({'rental_price': rental_price})

    def fit(self, from_data, to_predict, data_analysis, kwargs):
        Y = get_y(from_data, to_predict)
        X = get_x(from_data)
        self.model.fit(X, Y)

                     
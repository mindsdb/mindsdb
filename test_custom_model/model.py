
from sklearn.linear_model import LinearRegression
import numpy as np
import pandas as pd
from mindsdb import ModelInterface

class Model(ModelInterface):
    def setup(self):
        print('Setting up model !')
        self.model = LinearRegression()

    def get_x(self, data):
        initial_price = np.array([int(x) for x in data['initial_price']])
        initial_price = initial_price.reshape(-1, 1)
        return initial_price

    def get_y(self, data, to_predict_str):
        to_predict = np.array(data[to_predict_str])
        return to_predict

    def predict(self, from_data, kwargs):
        initial_price = self.get_x(from_data)
        rental_price = self.model.predict(initial_price)
        df = pd.DataFrame({'rental_price': rental_price})
        return df

    def fit(self, from_data, to_predict, data_analysis, kwargs):
        Y = self.get_y(from_data, to_predict)
        X = self.get_x(from_data)
        self.model.fit(X, Y)

                     
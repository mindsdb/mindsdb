from mindsdb import Predictor
import pandas as pd
import numpy as np
import sys
from sklearn.metrics import accuracy_score


class Insurance:

    def __init__(self):
       self.mindsDb = Predictor(name='insurance1')

    def insurance_train(self):
       self.mindsDb.learn(to_predict='PolicyStatus', from_data='insu_train_indep_dep.csv')

    def insurance_predict(self):
        df = pd.read_csv('insu_test_indep_dep.csv')

        y_real = list(df['PolicyStatus'])

        results = self.mindsDb.predict(when_data="insu_test_indep_dep.csv")

        y_pred = []
        for row in results:
           y_pred.append(row['PolicyStatus'])

        acc_score = accuracy_score(y_real, y_pred, normalize=True)
        acc_pct = round(acc_score * 100)
        print(f'Accuracy of : {acc_pct}%')


if __name__ == "__main__":
   tTest = Insurance()
   tTest.insurance_train()
   tTest.insurance_predict()

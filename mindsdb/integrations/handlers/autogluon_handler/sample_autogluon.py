from autogluon.tabular import TabularDataset, TabularPredictor
from sklearn.model_selection import train_test_split
import pandas as pd
import os
import glob

def sample_autogluon():
    train_data = TabularDataset('https://autogluon.s3.amazonaws.com/datasets/Inc/train.csv')
    subsample_size = 500  # subsample subset of data for faster demo, try setting this to much larger values
    train_data = train_data.sample(n=subsample_size, random_state=0)
    print(train_data.head(4))
    label = 'class'
    print("Summary of class variable: \n", train_data[label].describe())
    store_path = os.environ.get('MINDSDB_STORAGE_DIR') or ''
    save_path = os.path.join(store_path,'agModels-predictClass')  # specifies folder to store trained models
    predictor = TabularPredictor(label=label, path=save_path).fit(train_data)

    test_data = TabularDataset('https://autogluon.s3.amazonaws.com/datasets/Inc/test.csv')
    y_test = test_data[label]  # values to predict
    test_data_nolab = test_data.drop(columns=[label])  # delete label column to prove we're not cheating
    test_data_nolab.head()
    predictor = TabularPredictor.load(
        save_path)  # unnecessary, just demonstrates how to load previously-trained predictor from file

    y_pred = predictor.predict(test_data_nolab)
    print("Predictions:  \n", y_pred)
    perf = predictor.evaluate_predictions(y_true=y_test, y_pred=y_pred, auxiliary_metrics=True)

    predictor.leaderboard(test_data, silent=True)

def train_books():
    df = pd.read_csv('data.csv')
    df = df.dropna()
    train, test = train_test_split(df, test_size=0.2)
    train.to_csv('data_train.csv', index=False)
    test.to_csv('data_test.csv', index=False)
    train_data = TabularDataset('data_train.csv')
    # subsample_size = 5000  # subsample subset of data for faster demo, try setting this to much larger values
    # train_data = train_data.sample(n=subsample_size, random_state=0)
    print(train_data.head(4))
    label = 'genre'
    print("Summary of class variable: \n", train_data[label].describe())
    store_path = os.environ.get('MINDSDB_STORAGE_DIR') or ''
    save_path = os.path.join(store_path,'agModels-predictClass')

    # AutoGluon will gauge predictive performance using evaluation metric: 'accuracy'
    # 	To change this, specify the eval_metric parameter of Predictor()
    predictor = TabularPredictor(label=label, path=save_path).fit(train_data)

    test_data = TabularDataset('data_test.csv')
    y_test = test_data[label]  # values to predict
    test_data_nolab = test_data.drop(columns=[label])  # delete label column to prove we're not cheating
    test_data_nolab.head()
    predictor = TabularPredictor.load(
        save_path)  # unnecessary, just demonstrates how to load previously-trained predictor from file

    y_pred = predictor.predict(test_data_nolab)

    for feat in predictor.features():
        datatype = predictor.feature_metadata_in.get_feature_type_raw(feat)
        if feat not in test_data_nolab.columns:
            test_data_nolab[feat] = pd.Series(dtype=datatype)
    test_data_nolab.set_index('summary')

    print("Predictions:  \n", y_pred)
    perf = predictor.evaluate_predictions(y_true=y_test, y_pred=y_pred, auxiliary_metrics=True)

    predictor.leaderboard(test_data, silent=True)
    print(test_data.head(4))
if __name__ == '__main__':
    sample_autogluon()
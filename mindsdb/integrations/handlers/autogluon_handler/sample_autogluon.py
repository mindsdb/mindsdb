from autogluon.tabular import TabularDataset, TabularPredictor


if __name__ == '__main__':
    train_data = TabularDataset('https://autogluon.s3.amazonaws.com/datasets/Inc/train.csv')
    subsample_size = 500  # subsample subset of data for faster demo, try setting this to much larger values
    train_data = train_data.sample(n=subsample_size, random_state=0)
    print(train_data.head(4))
    label = 'class'
    print("Summary of class variable: \n", train_data[label].describe())
    save_path = 'agModels-predictClass'  # specifies folder to store trained models
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

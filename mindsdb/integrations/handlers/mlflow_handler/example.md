# Preliminaries

To use MLFlow-served models through MindsDB, you need to:

1) Train a model via a wrapper class that inherits from `mlflow.pyfunc.PythonModel`. Should expose a `predict()` method that returns the predicted output for some input data when called. 
   
(Important: ensure that the python version specified for conda env matches the one used to actually train the model).

2) Start the MLFlow server:
mlflow server -p 5001 --backend-store-uri sqlite:////path/to/mlflow.db --default-artifact-root ./artifacts --host 0.0.0.0

3) Serve the trained model: 
mlflow models serve --model-uri ./model_folder_name


# MindsDB example commands

-- Create a model that registers an MLFlow served model as an AI Table

CREATE MODEL mindsdb.test
PREDICT target
USING
engine='mlflow',  -- calls this handler
model_name='model_folder_name',
mlflow_server_url='http://0.0.0.0:5001/',  -- match port with mlflow server
mlflow_server_path='sqlite:////path/to/mlflow.db',
predict_url='http://localhost:5000/invocations';  -- match port with `mlflow serve`


-- Check model status

SELECT * FROM mindsdb.models WHERE name='test';  -- will appear as `complete` if import process finished successfully


-- Predict using synthetic data

SELECT target
FROM mindsdb.test
WHERE text='The tsunami is coming, seek high ground';  -- gets predictions for the input data


-- Batch prediction joining with another table

SELECT t.text, m.predict
FROM mindsdb.test as m
JOIN files.some_text as t;

## PyCaret Handler

PyCaret ML handler for MindsDB.

## PyCaret

PyCaret is an open-source, low-code machine learning library in Python that automates machine learning workflows.

## Example Usage

### Creation

Required parameters:
- `model_type`: the type of model that you want to build
- `model_name`: you can pass in supported models using this. eg. supported models for regression can be found [here](https://pycaret.readthedocs.io/en/latest/api/regression.html#pycaret.regression.create_model). You can also set it to `best` to generate the best model (only supported for classification, regression and time_series)

In addition to required parameters, there are 3 categories of optional parameters `setup`, `create` and `predict`. These are passed in during various stages of model development (see below). You have to prefix the arguments with one of these categories to pass in during the workflow.
- `setup_*`: these are passed to `setup()` function while creating model. You can find these in PyCaret's documentation. eg. For regression, the setup function's arguments are documented [here](https://pycaret.readthedocs.io/en/latest/api/regression.html#pycaret.regression.RegressionExperiment.setup).
- `create_*`: these are passed into `create_model()` or `compare_models()` function depending on the `model_name`. For classification you can find the docs [here](https://pycaret.readthedocs.io/en/latest/api/classification.html#pycaret.classification.create_model).
- `predict_*`: these are passed into `predict_model()` function of PyCaret. eg. You can find the documentation for classification [here](https://pycaret.readthedocs.io/en/latest/api/classification.html#pycaret.classification.predict_model).

These are the supported types of models (`model_type`):
- `classification`
- `regression`
- `time_series`
- `clustering`
- `anomaly`

Below is the example for creating a classification model

~~~sql
CREATE MODEL my_pycaret_class_model
FROM irisdb
    (SELECT SepalLengthCm, SepalWidthCm, PetalLengthCm, PetalWidthCm, Species FROM Iris)
PREDICT Species
USING 
  engine = 'pycaret',
  model_type = 'classification',
  model_name = 'xgboost',
  setup_session_id = 123;
~~~~

### Prediction

You can predict using normal mindsdb syntax like so:

~~~sql
SELECT t.Id, m.prediction_label, m.prediction_score
FROM irisdb.Iris as t
JOIN my_pycaret_class_model AS m;
~~~~

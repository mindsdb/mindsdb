# Autogluon Handler

The Autogluon ML handler integrates Autogluon with MindsDB. You can use it to create and train machine learning models with Autogluon for your existing data. 

## Autogluon

Autogluon is an open-source machine learning library designed to make machine learning easy and accessible. It offers automatic machine learning (AutoML) capabilities, allowing you to quickly build and train machine learning models without requiring in-depth expertise in machine learning.

The Autogluon Python SDK can be found at [https://github.com/autogluon/autogluon](https://github.com/autogluon/autogluon).

## Example Usage
To create an ML Engine with the new `autogluon` engine, you can use the following SQL command:

```sql
CREATE ML_ENGINE autoglucon_engine FROM autogluon;
```

Next, you can create a machine learning model using Autogluon with your dataset:

```sql

CREATE MODEL mindsdb.autoglucon_predictor
FROM files
    (SELECT * FROM files.red_wine_quality LIMIT 100) 
PREDICT quality AS quality
USING
engine='autoglucon_engine';
```

Here, replace `files.redwine_quality` with the your data source, and specify the appropriate columns for prediction (replace `quality` with your target label accordingly).

To check the status of your Autogluon model, use the following SQL query:

```sql
SELECT *
FROM mindsdb.models
WHERE name = 'autoglucon_predictor';
```

And to make predictions using your Autogluon model, you can use SQL queries like this:

```sql
SELECT m.quality
FROM files.red_wine_quality AS t
JOIN mindsdb.autoglucon_predictor AS m;
```


Remember to replace data source  and model name with your specific dataset and prediction column.

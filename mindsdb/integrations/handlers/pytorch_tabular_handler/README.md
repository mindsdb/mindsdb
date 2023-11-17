# Briefly describe what ML framework does this handler integrate to MindsDB, and how?
PyTorch Tabular aims to make Deep Learning with Tabular data easy and accessible to real-world cases and research alike. The core principles behind the design of the library are:

- Low Resistance Useability
- Easy Customization
- Scalable and Easier to Deploy
It has been built on the shoulders of giants like PyTorch(obviously), and PyTorch Lightning.

Call this handler with
`USING ENGINE='pytorch_tabular'` 

# Why is this integration useful? What does the ideal predictive use case for this integration look like? When would you definitely not use this integration?
PyTorch Tabular attempts to make the “software engineering” part of working with Neural Networks as easy and effortless as possible and let you focus on the model. I also hopes to unify 
the different developments in the Tabular space into a single framework with an API that will work with different state-of-the-art models. This integration handles the continuous and categorical
data automatically without the user needing to encode it. 

This integration uses very early version of pytorch which is `torch==1.13.1` which may not be supported by other integrations in Mindsdb.

# What are the recommended system specifications for models created with this framework?
Currently this integration only supports Tabnet model. [TabTransformer](https://arxiv.org/abs/1908.07442) is an adaptation of the Transformer 
model for Tabular Data which creates contextual representations for categorical features.

Furthur deep learning models can be added on depend and future version release

# To what degree can users control the underlying framework by passing parameters via the USING syntax?
Currently the handler is able to handle all the parameters available [here](https://pytorch-tabular.readthedocs.io/en/latest/#usage)

```
CREATE MODEL mindsdb.housing_tabular
FROM files (
SELECT * FROM California_Housing
)
PREDICT median_house_value
USING
    engine='pytorch_tabular',
    target = 'medain_house_value',
    initialization = 'xavier',
    task='regression',
    continuous_cols=["longitude", "latitude", "housing_median_age", "total_rooms", "total_bedrooms", "population", "households", "median income"],
    categorical_cols = ["ocean_proximity"],
    epochs = 5,
    batch_size = 32
```

# Does this integration offer model explainability or insights via the DESCRIBE syntax?
Yes this integration offers model explainability via the DESCRIBE syntax. 

# Does this integration support fine-tuning pre-existing models (i.e. is the update() method implemented)? Are there any caveats?
Not implemented yet.

# Any directions for future work in subsequent versions of the handler?
Implement the other models available in Pytorch Tabular.

# Please provide a minimal SQL example that uses this ML engine (pointers to integration tests in the PR also valid)
In the below example we use the famous California Housing dataset to predict the median house value. 

```
CREATE MODEL mindsdb.housing_tabular
FROM files (
SELECT FROM California_Housing
)
PREDICT median_house_value
USING
    engine='pytorch_tabular',
    target = 'medain_house_value',
    initialization = 'xavier',
    task='regression',
    continuous_cols=["longitude", "latitude", "housing_median_age", "total_rooms", "total_bedrooms", "population", "households", "median income"],
    categorical_cols = ["ocean_proximity"],
    epochs = 5,
    batch_size = 32cd 
```

Get predictions from the handler
```
SELECT median_house_value_prediction FROM mindsdb.housing_tabular
WHERE
AND longitude = -121
AND latitude=37 AND housing_median_age=40
AND total_rooms=880 total bedrooms=120
AND population=500
AND households=200
AND median income=8.5
AND ocean_proximity= 'NEAR BAY';
```

Get batch predictions from the handler
```
SELECT m.*
FROM files. California Housing as t 
JOIN mindsdb.housing_tabular as m
```
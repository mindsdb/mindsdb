
In MindsDB, the underlying automl models are based on Lightwood. This library generates model automatically based on the data and the problem definition, but the default configuration can be overridden.

# USING statement

The `USING ...` statement provides the option to configure a model to be trained.

## Syntax:

```sql
CREATE PREDICTOR [name_of_your _predictor]
FROM [name_of_your_integration]
    (SELECT * FROM your_database.your_data_table)
PREDICT [data_target_column]
USING 
[parameter] = ['parameter_value1']
;
```

# Parameters:

## THE ENCODER KEY (USING encoders)

Grants access to configure how each column is encoded; by default, the auto_ML will try to get the best match for the data.


```sql
... 
USING 
encoders.[column_name].module='value'
;
```

To learn more about how encoders work and their options, go [here](https://lightwood.io/encoder.html).

## THE MODEL KEY (USING model)

Allows you to specify what type of Machine Learning algorithm to learn from the encoder data.

```sql
... 
USING 
model.args [].=''
;
```

To learn more about all the model options, go [here](https://lightwood.io/mixer.html).
# Commmon Parameters Used

| Parameter   |  function  |
|----------|:-------------:|
| 'use_gpu' |  boolean  |
| 'encoders.[column]' |  boolean  |
| 'encoders.[column]' |  boolean  |


# Actual example 

```sql
CREATE PREDICTOR home_rentals_predictor 
FROM my_db_integration (
    SELECT * FROM home_rentals
) PREDICT rental_price
USING 
    /* Change the encoder for a column */
    encoders.location.module='CategoricalAutoEncoder',
    /* Change the encoder for another colum (the target) */
    encoders.rental_price.module = 'NumericEncoder',
    /* Change the arguments that will be passed to that encoder */
    encoders.rental_price.args.positive_domain = 'True',
    /* Set the list of models lightwood will try to use to a single one, a Light Gradient Boosting Machine.*/
    model.args='{"submodels": [{"module": "LightGBM", "args": {"stop_after": 12, "fit_on_dev": true}}]}';
;
```


If you're unsure how to configure a model to solve your problem, feel free to ask us how to do it on the community [Slack workspace](https://join.slack.com/t/mindsdbcommunity/shared_invite/zt-o8mrmx3l-5ai~5H66s6wlxFfBMVI6wQ).
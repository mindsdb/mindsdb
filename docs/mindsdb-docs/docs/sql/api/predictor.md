# CREATE PREDICTOR Statement

The `CREATE PREDICTOR` statement is used to train a new model. The basic syntax for training a model is:

```sql
CREATE PREDICTOR mindsdb.predictor_name
FROM integration_name 
(SELECT column_name, column_name2 FROM table_name) as ds_name
PREDICT column_name as column_alias;
```

* `CREATE PREDICTOR mindsdb.predictor_name` - where `predictor_name` is the name of the model.
* `FROM integration_name (select column_name, column_name2 FROM table_name)` - where `integration_name` is the name of the [datasource](/connect/#create-new-datasource), where `(select column_name, column_name2 FROM table_name)` is the SELECT statement for selecting the data. If you want to change the default name of the datasource you can use the alias `as ds_name`.
* `PREDICT column_name` - where `column_name` is the column name of the target variable. If you want to change the name of the target variable you can use the `as column_alias`.

### Example Data

The Home Rentals dataset contains prices of properties from a metropolitan area in the US. This table will be used in all of the documentation examples.

{{ read_csv('https://raw.githubusercontent.com/mindsdb/mindsdb-examples/master/classics/home_rentals/dataset/train.csv', nrows=5) }}

### Create Predictor example
This example shows how you can train a Machine Learning model called `home_rentals_model` to predict the rental prices for real estate properties inside the dataset.

```sql
CREATE PREDICTOR mindsdb.home_rentals_model
FROM db_integration (SELECT * FROM house_rentals_data) as rentals
PREDICT rental_price as price;
```

With this, we will use all columns from the dataset above to predict the value in `rental_price`. 


### Create from a file datasource
If you have uploaded a file from the GUI, you can specify `files` as the datasource:

```sql
CREATE PREDICTOR mindsdb.home_rentals_model
FROM files (SELECT * FROM house_rentals_data) as rentals
PREDICT rental_price as price;
```

### SELECT Predictor status

After you run the `CREATE PREDICTOR` statement, you can check the status of the training model, by selecting from `mindsdb.predictors` table:

```sql
SELECT * FROM mindsdb.predictors WHERE name='predictor_name';
```

### SELECT Predictor example

To check the training status for the `home_rentals_model` run:

```sql
SELECT * FROM mindsdb.predictors WHERE name='home_rentals_model';
```


## Time Series keywords

To train a timeseries model, MindsDB provides additional keywords.

* `ORDER BY` -  keyword is used as the column that defines the time series order by, these can be a date, or anything that defines the sequence of events to order the data by descending (DESC) or ascending (ASC) order. (The default order will always be `ASC`).

* `WINDOW` - keyword specifies the number of rows to "look back" into when making a prediction after the rows are ordered by the order_by column and split into groups. This could be used to specify something like "Always use the previous 10 rows". 

* `HORIZON` - (OPTIONAL, default value is 1) keyword specifies the number of future predictions. 

* `GROUP BY` - (OPTIONAL) keyword is used to group the rows that make a partition, for example, if you want to forecast inventory for all items in a store, you can partition the data by product_id, meaning that each product_id has its own time series. 

```sql
CREATE PREDICTOR mindsdb.predictor_name
FROM db_integration 
(SELECT sequential_column, partition_column, other_column, target_column FROM table_name) as ds_name
PREDICT target_column AS column_alias

ORDER BY sequantial_column
GROUP BY partition_column

WINDOW 10
HORIZON 5;
```

### Time Series example

The following example trains the new `inventory_model` model which can predicts the `units_in_inventory` for the next 7 days, taking into account the historical inventory in the past 20 days for each 'procuct_id'

```sql
CREATE PREDICTOR mindsdb.inventory_model
FROM db_integration
(SELECT * FROM inventory) as inventory
PREDICT units_in_inventory as predicted_units_in_inventory

ORDER BY date,
GROUP BY product_id,

WINDOW 20
HORIZON 7

```

## USING statement

In MindsDB, the underlying AutoML models are based on Lightwood. This library generates models automatically based on the data and a declarative problem definition, but the default configuration can be overridden.

The `USING ...` statement provides the option to configure a model to be trained with specific options.

## Syntax

```sql
CREATE PREDICTOR [mindsdb.name_of_your_predictor]
FROM [name_of_your_integration]
    (SELECT * FROM your_database.your_data_table)
PREDICT [data_target_column]
USING 
[parameter] = ['parameter_value1']
;
```

There are several high-level keys that can be specified with `USING`. Here, we explore a couple of them.

## Encoder key (USING encoders)

Grants access to configure how each column is encoded. By default, the AutoML engine will try to get the best match for the data.


```sql
... 
USING 
encoders.[column_name].module='value'
;
```

To learn more about how encoders work and their options, go [here](https://lightwood.io/encoder.html).

## Model key (USING model)

Allows you to specify what type of Machine Learning algorithm to learn from the encoder data.

```sql
... 
USING 
model.args='{"key": value}'
;
```

To learn more about all the model options, go [here](https://lightwood.io/mixer.html).

## Other keys

We support JSON-like syntax as well as a dot access notation for said JSON. In that sense, it is equivalent to write `USING a.b.c=5` or `USING a = {"b": {"c": 5}}`.

You have a lot of options in terms of what you can configure with `USING`. Since lightwood model are fully-generated by a JSON configuration we call "JsonAI", and with the `USING` statement you can modify all bits of that. The most common usecases for configuring predictors will be listed and explained in the example below. To see all options available in detail, you should checkout the lightwood docs about [`JsonAI`](https://lightwood.io/api/types.html#api.types.JsonAI).

## Example 

We will use the home rentals dataset, specifying particular encoders for some of the columns and a LightGBM model.

```sql
CREATE PREDICTOR mindsdb.home_rentals_predictor 
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
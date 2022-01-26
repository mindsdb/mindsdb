# CREATE PREDICTOR Statement

The `CREATE PREDICTOR` statement is used to train new model. The basic syntax for training the model is:

```sql
CREATE PREDICTOR predictor_name
FROM integration_name 
(SELECT column_name, column_name2 FROM table_name) as ds_name
PREDICT column_name as column_alias;
```

* `CREATE PREDICTOR predictor_name` - where `predictor_name` is the name of the model.
* `FROM integration_name (select column_name, column_name2 FROM table_name)` - where `integration_name` is the name of the [datasource](/connect/#create-new-datasource), where `(select column_name, column_name2 FROM table_name)` is the SELECT statement for selecting the data. If you want to change the default name of the datasource you can use the alias `as ds_name`.
* `PREDICT column_name` - where `column_name` is the column name of the target variable. If you want to change the name of the target variable you can use the `as column_alias`.

### Example Data

The below database table contains prices of properties from a metropolitan area in the US. This table will be used in all of the docs examples.

{{ read_csv('https://raw.githubusercontent.com/mindsdb/mindsdb-examples/master/classics/home_rentals/dataset/train.csv', nrows=5) }}


### Create Predictor example
This example shows how you can train the Machine Learning Model called `home_rentals_model` to predict the rentals price from the above data.

```sql
CREATE PREDICTOR home_rentals_model
FROM db_integration (SELECT * FROM house_rentals_data) as rentals
PREDICT rental_price as price;
```
### SELECT Predictor status

After you run the `CREATE Predictor` statement, you can check the status of the training model, by selecting from `mindsdb.predictors` table:

```sql
SELECT * FROM mindsdb.predictors WHERE name='predictor_name';
```

### SELECT Predictor example

To check the training status for the `home_rentals_model` run:

```sql
SELECT * FROM mindsdb.predictors WHERE name='home_rentals_model';
```


## USING keyword

The `USING` keyword accepts arguments as a JSON format where additional arguments can be provided to the `CREATE PREDICTOR` statement as:

* `stop_train_in_x_seconds` - Stop model training after X seconds.
* `use_gpu` - Switch between training on CPU or GPU (True|False).
* `sample_margin_of_error` - The amount of random sampling error in results (0 - 1)
* `ignore_columns` - Columns to be removed from the model training.
* `is_timeseries` - Training from time series data (True|False).

```sql
CREATE PREDICTOR predictor_name
FROM integration_name 
(SELECT column_name, column_name2 FROM table_name) as ds_name
PREDICT column_name as column_alias
USING {"ignore_columns": "column_name3"};
```

## USING example

The following example trains the new `home_rentals_model` model which predicts the `rental_price` and removes the number of bathrooms.

```sql
CREATE PREDICTOR home_rentals_model
FROM db_integration 
(SELECT * FROM house_rentals_data) as rentals
PREDICT rental_price as price
USING {"ignore_columns": "number_of_bathrooms"};
```

## Time Series keywords

To train a timeseries model, MindsDB provides additional keywords.
* `ORDER BY` -  keyword is used as the column that defines the time series order by, these can be a date, or anything that defines the sequence of events to order the data by descending (DESC) or ascending (ASC) order. The default order will always be `ASC`
* `WINDOW` - keyword specifies the number of rows to "look back" into when making a prediction after the rows are ordered by the order_by column and split into groups. This could be used to specify something like "Always use the previous 10 rows". 
* `HORIZON` - (OPTIONAL, default value is 1) keyword specifies the number of future predictions. 
* `GROUP BY` - (OPTIONAL) keyword is used to group the rows that make a partition, for example, if you want to forecast inventory for all items in a store, you can partition the data by product_id, meaning that each product_id has its own time series. 

```sql
CREATE PREDICTOR predictor_name
FROM db_integration 
(SELECT sequential_column, partition_column, other_column, target_column FROM table_name) as ds_name
PREDICT target_column AS column_alias
GROUP BY partition_column
ORDER BY sequantial_column
WINDOW 10
HORIZON 7;
```

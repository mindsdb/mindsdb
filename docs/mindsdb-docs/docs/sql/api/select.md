# `#!sql SELECT` statement

## Description

The `#!sql SELECT` statement fetches predictions from the model table. The data is returned on the fly and not saved.

But there are ways to save predictions data! You can save your predictions as a view using the [`CREATE VIEW`](/sql/create/view/) statement. Please note that a view is a saved query and does not store data like a table. Another way is to insert your predictions into a table using the [`INSERT INTO`](/sql/api/insert/) statement.

## Syntax

### Single Prediction

Here is the syntax for fetching a single prediction from the model table:

```sql
SELECT [target_variable], [target_variable]_explain
FROM mindsdb.[predictor_name]
WHERE [column]=[value] 
AND [column]=[value];
```

!!! warning "Grammar Matters"
    Here are some points to keep in mind while writing queries in MindsDB:<br/>
    &nbsp;&nbsp;&nbsp;1. The `[column]=[value]` pairs may be joined by `AND` or `OR` keywords.<br/>
    &nbsp;&nbsp;&nbsp;2. Do not use any quotations for numerical values.<br/>
    &nbsp;&nbsp;&nbsp;3. Use single quotes for strings.

On execution, we get:

```sql
+-------------------+-----------------------------------------------------------------------------------------------------------------------------------------------+
| [target_variable] | [target_variable]_explain                                                                                                                     |
+-------------------+-----------------------------------------------------------------------------------------------------------------------------------------------+
| [predicted_value] | {"predicted_value": 4394, "confidence": 0.99, "anomaly": null, "truth": null, "confidence_lower_bound": 4313, "confidence_upper_bound": 4475} |
+-------------------+-----------------------------------------------------------------------------------------------------------------------------------------------+
```

Where:

| Name                                     | Description                                                                                                                                                                          |
| ---------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `[target_variable]`                      | Name of the column to be predicted.                                                                                                                                                  |
| `[target_variable]_explain`              | Object of the JSON type that contains the `predicted_value` and additional information such as `confidence`, `anomaly`, `truth`, `confidence_lower_bound`, `confidence_upper_bound`. |
| `[predictor_name]`                       | Name of the model used to make the prediction.                                                                                                                                       |
| `#!sql WHERE [column]=[value] AND ...`   | `#!sql WHERE` clause used to pass the data values for which the prediction is made.                                                                                                  |

### Bulk Predictions

Here is the syntax for making predictions in bulk by joining the data source table with the model table:

```sql
SELECT m.[target_variable], t.[column1], t.[column2]
FROM [integration_name].[table_name] AS t
JOIN mindsdb.[predictor_name] AS m;
```

On execution, we get:

```sql
+----------------------+-------------+-------------+
| [target_variable]    | [column1]   | [column2]   |
+----------------------+-------------+-------------+
| [predicted_value_1]  | value1.1    | value2.1    |
| [predicted_value_2]  | value1.2    | value2.2    |
| [predicted_value_3]  | value1.3    | value2.3    |
| [predicted_value_4]  | value1.4    | value2.4    |
| [predicted_value_5]  | value1.5    | value2.5    |
+----------------------+-------------+-------------+
```

Where:

| Name                                   | Description                                                                                                                         |
| -------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------|
| `m.[target_variable]`                  | Name of the column to be predicted. The `m.` in front indicates that this column comes from the `mindsdb.[predictor_name]` table.   |
| `t.[column1], t.[column2]`             | Columns from the data source table (`[integration_name].[table_name]`) that you want to see in the output.                          |
| `[integration_name].[table_name]`      | Data source table that is joined with the model table (`mindsdb.[predictor_name]`).                                                 |
| `[predictor_name]`                     | Name of the model used to make predictions.                                                                                         |

Please note that in the case of bulk predictions, we do not pass the data values for which the prediction is made. It is because bulk predictions use all data available in the data source table.

## Example

### Single Prediction

Let's predict the `rental_price` value using the `home_rentals_model` model for the property having `sqft=823`, `location='good'`, `neighborhood='downtown'`, and `days_on_market=10`.

```sql
SELECT sqft, location, neighborhood, days_on_market, rental_price, rental_price_explain
FROM mindsdb.home_rentals_model1
WHERE sqft=823
AND location='good'
AND neighborhood='downtown'
AND days_on_market=10;
```

On execution, we get:

```sql
+-------+----------+--------------+----------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------+
| sqft  | location | neighborhood | days_on_market | rental_price | rental_price_explain                                                                                                                          |
+-------+----------+--------------+----------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------+
| 823   | good     | downtown     | 10             | 4394         | {"predicted_value": 4394, "confidence": 0.99, "anomaly": null, "truth": null, "confidence_lower_bound": 4313, "confidence_upper_bound": 4475} |
+-------+----------+--------------+----------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------+
```

### Bulk Predictions

Now let's make bulk predictions to predict the `rental_price` value using the `home_rentals_model` model joined with the data source table.

```sql
SELECT t.sqft, t.location, t.neighborhood, t.days_on_market, t.rental_price AS real_price,
       m.rental_price AS predicted_rental_price
FROM example_db.demo_data.home_rentals AS t
JOIN mindsdb.home_rentals_model AS m
LIMIT 5;
```

On execution, we get:

```sql
+-------+----------+-----------------+----------------+--------------+-----------------------------+
| sqft  | location | neighborhood    | days_on_market | real_price   | predicted_rental_price      |
+-------+----------+-----------------+----------------+--------------+-----------------------------+
| 917   | great    | berkeley_hills  | 13             | 3901         | 3886                        |
| 194   | great    | berkeley_hills  | 10             | 2042         | 2007                        |
| 543   | poor     | westbrae        | 18             | 1871         | 1865                        |
| 503   | good     | downtown        | 10             | 3026         | 3020                        |
| 1066  | good     | thowsand_oaks   | 13             | 4774         | 4748                        |
+-------+----------+-----------------+----------------+--------------+-----------------------------+
```

## Select from integration

### Simple select

In this example query contains only tables from one integration 
and therefore will be sent to integration database
(integration name will be cut from table name)
```sql
SELECT location, max(sqft)
FROM example_db.demo_data.home_rentals 
GROUP BY location
LIMIT 5;
```


### Raw select from integration

It is also possible to send raw query to integration. 
It can be useful when query to integration can not be parsed as sql

Syntax:

```sql
SELECT ... FROM <integration_name> ( <raw query> ) 
```

Example of select from mongo integration using mongo query
```sql
SELECT * FROM mongo (
 db.house_sales2.find().limit(1) 
)
```

## Complex queries

1. Subselect on data from integration.

It can be useful in cases when ingration engine doesn't support some functions, for example grouping.
In that case all data from raw select are passed to mindsdb and then subselect performs on them inside mindsdb

```sql
SELECT type, max(bedrooms), last(MA)
FROM mongo (
 db.house_sales2.find().limit(300) 
) GROUP BY 1
```

2. Unions

It is possible to use UNION / UNION ALL operators.
It this case every subselect from union will be fetched and merged to one result-set on mindsdb side  

```sql
 SELECT 
  data.time as date, data.target
 FROM datasource.table_name as data
UNION ALL
 SELECT
  model.time as date, model.target as target
 FROM mindsdb.model as model 
  JOIN datasource.table_name as t
 WHERE t.time > LATEST AND t.group = 'value';
```

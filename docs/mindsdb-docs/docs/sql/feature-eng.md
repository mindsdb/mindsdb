# Feature Engineering in MindsDB

The more data you have, the more accurate predictions you get.

We recommend you provide the predictor with as many historical data rows and data columns as possible to make your predictions even more accurate. The examples presented here prove this hypothesis.

If you want to follow the examples, please [sign up for the MindsDB Cloud account here](https://cloud.mindsdb.com/register).

## Prerequisites

The base table is available in the `example_db` integration in the MindsDB Cloud Editor. In order to be able to use it, you must first create a database like this:

```sql
CREATE DATABASE example_db
WITH ENGINE = "postgres",
PARAMETERS = {
    "user": "demo_user",
    "password": "demo_password",
    "host": "3.220.66.106",
    "port": "5432",
    "database": "demo"
};
```

On execution, we get:

```sql
Query OK, 0 rows affected (x.xxx sec)
```

Once that's done, you can run the following commands with us.

## Example: Adding More Data Columns

### Introduction

Here, we'll create several predictors using the same table, increasing the number of data columns with each step.

We start with the base table and create a predictor based on it. Then we add two columns to our base table and again create a predictor based on the enhanced table. At last, we add another two columns and create a predictor.

By comparing the accuracies of the predictors, we'll find that more data results in more accurate predictions.

Let's get started.

### Let's Run the Codes

Here, we go through the codes for the base table and enhanced base tables simultaneously.

#### Data Setup

Let's prepare and verify the data. Here, we create the views and query them to ensure the input for the predictors is in order.

=== "Using the Base Table"

    Let's start by querying the data from the `example_db.demo_data.used_car_price` table, which is our base table.

    ```sql
    SELECT *
    FROM example_db.demo_data.used_car_price
    LIMIT 5;
    ```

    On execution, we get:

    ```sql
    +-----+----+-----+------------+-------+--------+---+----+----------+
    |model|year|price|transmission|mileage|fueltype|tax|mpg |enginesize|
    +-----+----+-----+------------+-------+--------+---+----+----------+
    | A1  |2017|12500|Manual      |15735  |Petrol  |150|55.4|1.4       |
    | A6  |2016|16500|Automatic   |36203  |Diesel  |20 |64.2|2         |
    | A1  |2016|11000|Manual      |29946  |Petrol  |30 |55.4|1.4       |
    | A4  |2017|16800|Automatic   |25952  |Diesel  |145|67.3|2         |
    | A3  |2019|17300|Manual      |1998   |Petrol  |145|49.6|1         |
    +-----+----+-----+------------+-------+--------+---+----+----------+
    ```

=== "Using the Base Table + 2 More Columns"

    Let's create a view based on the `example_db.demo_data.used_car_price` table, and add two more columns. Please note that we replace the `mpg` column with the `kml` column.

    ```sql
    CREATE VIEW used_car_price_plus_2_columns (
    SELECT * FROM example_db (
        SELECT 
        model, 
        year, 
        price, 
        transmission, 
        mileage, 
        fueltype, 
        tax,
        enginesize, 
        ROUND(CAST((mpg / 2.3521458) AS numeric), 1) AS kml, -- mpg (miles per galon) is replaced with kml (kilometers per liter)
        (date_part('year', CURRENT_DATE)-year) AS years_old -- age of a car
        FROM demo_data.used_car_price
    )
    );
    ```

    On execution, we get:

    ```sql
    Query OK, 0 rows affected (x.xxx sec)
    ```

    Let's query the newly created view.

    ```sql
    SELECT *
    FROM views.used_car_price_plus_2_columns
    LIMIT 5;
    ```

    On execution, we get:

    ```sql
    +-----+----+-----+------------+-------+--------+---+----+----------+----+---------+
    |model|year|price|transmission|mileage|fueltype|tax|mpg |enginesize|kml |years_old|
    +-----+----+-----+------------+-------+--------+---+----+----------+----+---------+
    | A1  |2017|12500|Manual      |15735  |Petrol  |150|55.4|1.4       |23.6|5        |
    | A6  |2016|16500|Automatic   |36203  |Diesel  |20 |64.2|2         |27.3|6        |
    | A1  |2016|11000|Manual      |29946  |Petrol  |30 |55.4|1.4       |23.6|6        |
    | A4  |2017|16800|Automatic   |25952  |Diesel  |145|67.3|2         |28.6|5        |
    | A3  |2019|17300|Manual      |1998   |Petrol  |145|49.6|1         |21.1|3        |
    +-----+----+-----+------------+-------+--------+---+----+----------+----+---------+
    ```

=== "Using the Base Table + 4 More Columns"

    Let's create a view based on the `example_db.demo_data.used_car_price` table, and add four more columns. Please note that we replace the `mpg` column with the `kml` column.

    ```sql
    CREATE VIEW used_car_price_plus_another_2_columns (
    SELECT * FROM example_db (
        SELECT 
        model, 
        year, 
        price, 
        transmission, 
        mileage, 
        fueltype, 
        tax,
        enginesize, 
        ROUND(CAST((mpg / 2.3521458) AS numeric), 1) AS kml, -- mpg (miles per galon) is replaced with kml (kilometers per liter)
        (date_part('year', CURRENT_DATE)-year) AS years_old, -- age of a car
        COUNT(*) OVER (PARTITION BY model, year) AS units_to_sell, -- how many units of a certain model are sold in a year
        ROUND((CAST(tax AS decimal) / price), 3) AS tax_div_price -- value of tax divided by price of a car
        FROM demo_data.used_car_price
    )
    );
    ```

    On execution, we get:

    ```sql
    Query OK, 0 rows affected (x.xxx sec)
    ```

    Let's query the newly created view.

    ```sql
    SELECT *
    FROM views.used_car_price_plus_another_2_columns
    LIMIT 5;
    ```

    On execution, we get:

    ```sql
    +-----+----+-----+------------+-------+--------+---+----+----------+----+---------+-------------+-------------+
    |model|year|price|transmission|mileage|fueltype|tax|mpg |enginesize|kml |years_old|units_to_sell|tax_div_price|
    +-----+----+-----+------------+-------+--------+---+----+----------+----+---------+-------------+-------------+
    | A1  |2010|9990 |Automatic   |38000  |Petrol  |125|53.3|1.4       |22.7|12       |1            |0.013        |
    | A1  |2011|6995 |Manual      |65000  |Petrol  |125|53.3|1.4       |22.7|11       |5            |0.018        |
    | A1  |2011|6295 |Manual      |107000 |Petrol  |125|53.3|1.4       |22.7|11       |5            |0.020        |
    | A1  |2011|4250 |Manual      |116000 |Diesel  |20 |70.6|1.6       |30.0|11       |5            |0.005        |
    | A1  |2011|6475 |Manual      |45000  |Diesel  |0  |70.6|1.6       |30.0|11       |5            |0.000        |
    +-----+----+-----+------------+-------+--------+---+----+----------+----+---------+-------------+-------------+
    ```

!!! note "Dropping a View"
    If you want to drop a view, run the command `DROP VIEW view_name;`.

#### Creating Predictors

Now, we create predictors based on the `example_db.demo_data.used_car_price` table and its extensions.

=== "Using the Base Table"

    ```sql
    CREATE PREDICTOR mindsdb.price_predictor
    FROM example_db
    (SELECT * FROM demo_data.used_car_price)
    PREDICT price;
    ```

    On execution, we get:

    ```sql
    Query OK, 0 rows affected (x.xxx sec)
    ```

=== "Using the Base Table + 2 More Columns"

    ```sql
    CREATE PREDICTOR mindsdb.price_predictor_plus_2_columns
    FROM views
    (SELECT * FROM used_car_price_plus_2_columns)
    PREDICT price;
    ```

    On execution, we get:

    ```sql
    Query OK, 0 rows affected (x.xxx sec)
    ```

=== "Using the Base Table + 4 More Columns"

    ```sql
    CREATE PREDICTOR mindsdb.price_predictor_plus_another_2_columns
    FROM views
    (SELECT * FROM used_car_price_plus_another_2_columns)
    PREDICT price;
    ```

    On execution, we get:

    ```sql
    Query OK, 0 rows affected (x.xxx sec)
    ```

!!! note "Dropping a Predictor"
    If you want to drop a predictor, run the command `DROP PREDICTOR predictor_name;`.

#### Predictor Status

Finally, let's check the predictor status whose value is `generating` at first, then `training`, and at last, `complete`.

=== "Using the Base Table"

    ```sql
    SELECT *
    FROM mindsdb.predictors
    WHERE name='price_predictor';
    ```

    On execution, we get:

    ```sql
    +---------------+--------+--------+---------+-------------+---------------+------+--------------------------------------+----------------+
    |name           |status  |accuracy|predict  |update_status|mindsdb_version|error |select_data_query                     |training_options|
    +---------------+--------+--------+---------+-------------+---------------+------+--------------------------------------+----------------+
    |price_predictor|complete|0.963   |price    |up_to_date   |22.10.2.1      |[NULL]|SELECT * FROM demo_data.used_car_price|                |
    +---------------+--------+--------+---------+-------------+---------------+------+--------------------------------------+----------------+
    ```

=== "Using the Base Table + 2 More Columns"

    ```sql
    SELECT *
    FROM mindsdb.predictors
    WHERE name='price_predictor_plus_2_columns';
    ```

    On execution, we get:

    ```sql
    +------------------------------+--------+--------+---------+-------------+---------------+------+-------------------------------------------+----------------+
    |name                          |status  |accuracy|predict  |update_status|mindsdb_version|error |select_data_query                          |training_options|
    +------------------------------+--------+--------+---------+-------------+---------------+------+-------------------------------------------+----------------+
    |price_predictor_plus_2_columns|complete|0.965   |price    |up_to_date   |22.10.2.1      |[NULL]|SELECT * FROM used_car_price_plus_2_columns|                |
    +------------------------------+--------+--------+---------+-------------+---------------+------+-------------------------------------------+----------------+
    ```

=== "Using the Base Table + 4 More Columns"

    ```sql
    SELECT *
    FROM mindsdb.predictors
    WHERE name='price_predictor_plus_another_2_columns';
    ```

    On execution, we get:

    ```sql
    +--------------------------------------+--------+--------+---------+-------------+---------------+------+---------------------------------------------------+----------------+
    |name                                  |status  |accuracy|predict  |update_status|mindsdb_version|error |select_data_query                                  |training_options|
    +--------------------------------------+--------+--------+---------+-------------+---------------+------+---------------------------------------------------+----------------+
    |price_predictor_plus_another_2_columns|complete|0.982   |price    |up_to_date   |22.10.2.1      |[NULL]|SELECT * FROM used_car_price_plus_another_2_columns|                |
    +--------------------------------------+--------+--------+---------+-------------+---------------+------+---------------------------------------------------+----------------+
    ```

### Accuracy Comparison

Once the training process of all three predictors completes, we see the accuracy values.

* For the base table, we get an accuracy value of `0.963`.
* For the base table with two more data columns, we get an accuracy value of `0.965`. The accuracy value increased, as expected.
* For the base table with four more data columns, we get an accuracy value of `0.982`. The accuracy value increased again, as expected.

### True vs Predicted Price Comparison

Let's compare how close the predicted price values are to the true price.

```sql
+-------+-------+---------------+-----------+-----------+--------------+----------------+----------------+---------------+
| model | year  | transmission  | fueltype  | mileage   | true_price   | pred_price_1   | pred_price_2   | pred_price_3  |
+-------+-------+---------------+-----------+-----------+--------------+----------------+----------------+---------------+
| A1    | 2017  | Manual        | Petrol    | 7620      | 14440        | 17268          | 17020          | 14278         |
| A6    | 2016  | Automatic     | Diesel    | 20335     | 18982        | 17226          | 17935          | 19016         |
| A3    | 2018  | Semi-Auto     | Diesel    | 9058      | 19900        | 25641          | 23008          | 21286         |
+-------+-------+---------------+-----------+-----------+--------------+----------------+----------------+---------------+
```

The prices predicted by the third predictor, having the highest accuracy value, are the closest to the true price, as expected.

## Example: Joining Data Tables

### Introduction

First, we'll use a table that contains a subset of columns from the `example_db.demo_data.used_car_price` table. Namely, it contains the `model`, `year`, `transmission`, `mileage`, `fueltype`, `price`, and `tax` columns. And we'll create a predictor based on this table.

Next, we'll join the remaining data from the `example_db.demo_data.used_car_price` table and create a predictor to see the impact on its strength.

Let's get started.

### Let's Run the Codes

Here, we go through the codes for the partial table and the full table after joining the data.

#### Data Setup

Here is the partial table:

```sql
SELECT *
FROM files.usedcarprice_1
LIMIT 5;
```

On execution, we get:

```sql
+-----+----+------------+-------+--------+-----+---+
|model|year|transmission|mileage|fueltype|price|tax|
+-----+----+------------+-------+--------+-----+---+
| A1  |2017|Manual      |15735  |Petrol  |12500|150|
| A6  |2016|Automatic   |36203  |Diesel  |16500|20 |
| A1  |2016|Manual      |29946  |Petrol  |11000|30 |
| A4  |2017|Automatic   |25952  |Diesel  |16800|145|
| A3  |2019|Manual      |1998   |Petrol  |17300|145|
+-----+----+------------+-------+--------+-----+---+
```

And here is the remaining data (the `mpg` and `enginesize` columns):

```sql
SELECT *
FROM files.usedcarprice_2
LIMIT 5;
```

On execution, we get:

```sql
+-----+----+------------+-------+--------+-----+----------+
|model|year|transmission|mileage|fueltype|mpg  |enginesize|
+-----+----+------------+-------+--------+-----+----------+
| A1  |2017|Manual      |15735  |Petrol  |55.4 |1.4       |
| A6  |2016|Automatic   |36203  |Diesel  |64.2 |2         |
| A1  |2016|Manual      |29946  |Petrol  |55.4 |1.4       |
| A4  |2017|Automatic   |25952  |Diesel  |67.3 |2         |
| A3  |2019|Manual      |1998   |Petrol  |49.6 |1         |
+-----+----+------------+-------+--------+-----+----------+
```

The two tables above will be joined on the `model`, `year`, `trasmission`, `mileage`, and `fueltype` columns.

After the `JOIN` operation, we get the original `example_db.demo_data.used_car_price` table:

```sql
SELECT *
FROM example_db.demo_data.used_car_price
LIMIT 5;
```

On execution, we get:

```sql
+-----+----+-----+------------+-------+--------+---+----+----------+
|model|year|price|transmission|mileage|fueltype|tax|mpg |enginesize|
+-----+----+-----+------------+-------+--------+---+----+----------+
| A1  |2017|12500|Manual      |15735  |Petrol  |150|55.4|1.4       |
| A6  |2016|16500|Automatic   |36203  |Diesel  |20 |64.2|2         |
| A1  |2016|11000|Manual      |29946  |Petrol  |30 |55.4|1.4       |
| A4  |2017|16800|Automatic   |25952  |Diesel  |145|67.3|2         |
| A3  |2019|17300|Manual      |1998   |Petrol  |145|49.6|1         |
+-----+----+-----+------------+-------+--------+---+----+----------+
```

#### Creating Predictors

Let's create a predictor for the partial table.

```sql
CREATE PREDICTOR mindsdb.price_predictor_partial_table
FROM files
  (SELECT * FROM usedcarprice_1)
PREDICT price;
```

On execution, we get:

```sql
Query OK, 0 rows affected (x.xxx sec)
```

Now, let's create a predictor for the table that is a `JOIN` between the two partial tables.

```sql
CREATE PREDICTOR mindsdb.price_predictor
FROM example_db
  (SELECT * FROM demo_data.used_car_price)
PREDICT price;
```

On execution, we get:

```sql
Query OK, 0 rows affected (x.xxx sec)
```

#### Predictor Status

Next, we check the status of both predictors.

We start with the predictor based on the partial table.

```sql
SELECT *
FROM mindsdb.predictors
WHERE name='price_predictor_partial_table';
```

On execution, we get:

```sql
+-----------------------------+--------+---------+-------+-------------+---------------+------+----------------------------+----------------+
|name                         |status  |accuracy |predict|update_status|mindsdb_version|error |select_data_query           |training_options|
+-----------------------------+--------+---------+-------+-------------+---------------+------+----------------------------+----------------+
|price_predictor_partial_table|complete|0.912    |price  |up_to_date   |22.10.2.1      |[NULL]|SELECT * FROM usedcarprice_1|                |
+-----------------------------+--------+---------+-------+-------------+---------------+------+----------------------------+----------------+
```

And now, for the predictor based on the full table after joining more data.

```sql
SELECT *
FROM mindsdb.predictors
WHERE name='price_predictor';
```

On execution, we get:

```sql
+----------------+--------+---------+-------+-------------+---------------+------+--------------------------------------+----------------+
|name            |status  |accuracy |predict|update_status|mindsdb_version|error |select_data_query                     |training_options|
+----------------+--------+---------+-------+-------------+---------------+------+--------------------------------------+----------------+
|price_predictor |complete|0.963    |price  |up_to_date   |22.10.2.1      |[NULL]|SELECT * FROM demo_data.used_car_price|                |
+----------------+--------+---------+-------+-------------+---------------+------+--------------------------------------+----------------+
```

### Accuracy Comparison

Once the training process of both predictors completes, we see the accuracy values.

* For the partial table, we get an accuracy value of `0.912`.
* For the full table after joining more data, we get an accuracy value of `0.963`. The accuracy value increased, as expected.

### True vs Predicted Price Comparison

Let's compare how close the predicted price values are to the true price.

```sql
+-------+-------+---------------+-----------+-----------+--------------+----------------+----------------+
| model | year  | transmission  | fueltype  | mileage   | true_price   | pred_price_1   | pred_price_2   |
+-------+-------+---------------+-----------+-----------+--------------+----------------+----------------+
| A1    | 2017  | Manual        | Petrol    | 7620      | 14440        | 18676          | 17268          |
| A5    | 2010  | Manual        | Petrol    | 76000     | 9495         | 7235           | 7656           |
| A3    | 2018  | Semi-Auto     | Diesel    | 9058      | 19900        | 31074          | 25641          |
+-------+-------+---------------+-----------+-----------+--------------+----------------+----------------+
```

The prices predicted by the second predictor, based on the full joined table, are the closest to the true price.

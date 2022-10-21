# Feature Engineering in MindsDB

The more data you have, the more accurate predictions you get.

We recommend you provide the predictor with as many historical data rows and data columns as possible to make your predictions even more accurate. The examples presented here prove this hypothesis.

If you want to follow the examples, please [sign up for the MindsDB Cloud account here](https://cloud.mindsdb.com/register).

## Example: Adding More Data Columns

### Introduction

Here, we'll create several predictors using the same table, increasing the number of data columns with each step.

We start with the base table and create a predictor based on it. Then we add two columns to our base table and again create a predictor based on the enhanced table. At last, we add another two columns and create a predictor.

By comparing the accuracies of the predictors, we'll find that more data results in more accurate predictions.

Let's get started.

### Prerequisites

Our base table is available in the `example_db` integration in the MindsDB Cloud Editor. In order to be able to use it, you must first create a database like this:

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

### Let's Run the Codes

Here, we go through the codes for the base table and enhanced base tables simulataneously.

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

    Let's create a view based on the `example_db.demo_data.used_car_price` table, and add two more columns.

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
        mpg,
        enginesize, 
        ROUND(CAST((mpg / 2.3521458) AS numeric), 1) AS kml, 
        (date_part('year', CURRENT_DATE)-year) AS years_old
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

    Let's create a view based on the `example_db.demo_data.used_car_price` table, and add four more columns.

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
        mpg,
        enginesize, 
        ROUND(CAST((mpg / 2.3521458) AS numeric), 1) AS kml, 
        (date_part('year', CURRENT_DATE)-year) AS years_old,
        COUNT(*) OVER (PARTITION BY model, year) AS units_to_sell,
        ROUND((CAST(tax AS decimal) / price), 3) AS tax_div_price
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

Once the training process of all three predictors completes, we see the accuracy values.

* For the base table, we get an accuracy value of `0.963`.
* For the base table with two more data columns, we get an accuracy value of `0.965`. The value is increased, as expected.
* For the base table with four more data columns, we get an accuracy value of `0.982`. The value is increased again, as expected.

## Example: Joining Data Tables

!!! warning "Example WIP"
    This example is a work in progress.

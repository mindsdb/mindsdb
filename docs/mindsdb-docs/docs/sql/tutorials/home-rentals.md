# Predicting Home Rental Prices with MindsDB

## Introduction

In this tutorial, we'll create, train, and query a machine learning model, which, in MindsDB language, is an `AI Table` or a `predictor`. We aim to predict the `rental_price` value for new properties based on their attributes, such as the number of rooms, area, or neighborhood.

Make sure you have access to a working MindsDB installation either locally or via [cloud.mindsdb.com](https://cloud.mindsdb.com/).

You can learn how to set up your account at MindsDB Cloud by following [this guide](https://docs.mindsdb.com/setup/cloud/). Another way is to set up MindsDB locally using [Docker](https://docs.mindsdb.com/setup/self-hosted/docker/) or [Python](https://docs.mindsdb.com/setup/self-hosted/pip/source/).

Let's get started.

## The Data

### Connecting the data

There are a couple of ways you can get the data to follow through with this tutorial.

=== "Connecting as a database via `#!sql CREATE DATABASE`"

    You can connect to a demo database that we've prepared for you. It contains the data used throughout this tutorial which is the `#!sql example_db.demo_data.home_rentals` table.

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

    Now you can run queries directly on the demo database. Let's preview the data that we'll use to train our predictor.

    ```sql
    SELECT * 
    FROM example_db.demo_data.home_rentals 
    LIMIT 10;
    ```

=== "Connecting as a file"

    You can download [the source file as a `.CSV` here](https://mindsdb-test-file-dataset.s3.amazonaws.com/home_rentals.csv) and then upload it via [MindsDB SQL Editor](https://docs.mindsdb.com/connect/mindsdb_editor/).

    Follow [this guide](https://docs.mindsdb.com/sql/create/file/) to find out how to upload a file to MindsDB.

    Now you can run queries directly on the file as if it were a table. Let's preview the data that we'll use to train our predictor.

    ```sql
    SELECT *
    FROM files.home_rentals
    LIMIT 10;
    ```

!!! Warning "From now on, we will use the `#!sql example_db.demo_data.home_rentals` table. Make sure you replace it with `files.home_rentals` if you connect the data as a file."

### Understanding the Data

We will use the home rentals dataset where each row represents one rental home. We will predict the `rental price` value for all the newly added properties in the following sections of this tutorial.

Below is the sample data stored in the `#!sql example_db.demo_data.home_rentals` table.

```sql
+-----------------+---------------------+------+----------+----------------+----------------+--------------+
| number_of_rooms | number_of_bathrooms | sqft | location | days_on_market | neighborhood   | rental_price |
+-----------------+---------------------+------+----------+----------------+----------------+--------------+
|               2 |                   1 |  917 | great    |             13 | berkeley_hills |         3901 |
|               0 |                   1 |  194 | great    |             10 | berkeley_hills |         2042 |
|               1 |                   1 |  543 | poor     |             18 | westbrae       |         1871 |
|               2 |                   1 |  503 | good     |             10 | downtown       |         3026 |
|               3 |                   2 | 1066 | good     |             13 | thowsand_oaks  |         4774 |
+-----------------+---------------------+------+----------+----------------+----------------+--------------+
```

Where:

| Column                | Description                                                                                  | Data Type           | Usage   |
| :-------------------- | :------------------------------------------------------------------------------------------- | ------------------- | ------- |
| `number_of_rooms`     | Number of rooms in a given house `[0,1,2,3]`                                                 | `integer`           | Feature |
| `number_of_bathrooms` | Number of bathrooms in a given house `[1,2]`                                                 | `integer`           | Feature |
| `sqft`                | Area of a given house in square feet                                                         | `integer`           | Feature |
| `location`            | Rating of the location of a given house `[poor, great, good]`                                | `character varying` | Feature |
| `days_on_market`      | Number of days a given house has been open to be rented                                      | `integer`           | Feature |
| `neighborhood`        | Neighborhood a given house is in `[alcatraz_ave, westbrae, ..., south_side, thowsand_oaks ]` | `character varying` | Feature |
| `rental_price`        | Rental price of a given house in dollars                                                     | `integer`           | Label   |

!!!Info "Labels and Features"

    A **label** is the thing we're predicting — the y variable in simple linear regression.
    A **feature** is an input variable — the x variable in simple linear regression.

## Training a Predictor Via [`#!sql CREATE PREDICTOR`](/sql/create/predictor)

Let's create and train your first machine learning predictor. For that, we are going to use the [`#!sql CREATE PREDICTOR`](/sql/create/predictor) syntax where we specify what sub-query to train `#!sql FROM` (features) and what we want to `#!sql PREDICT` (labels).

```sql
CREATE PREDICTOR mindsdb.home_rentals_predictor
FROM example_db
  (SELECT * FROM demo_data.home_rentals)
PREDICT rental_price;
```

We use all of the columns as features, except for the `rental_price` column whose value is going to be predicted.

## Checking the Status of a Predictor

A predictor may take a couple of minutes for the training to complete. You can monitor the status of your predictor by using this SQL command:

```sql
SELECT status
FROM mindsdb.predictors
WHERE name='home_rentals_predictor';
```

If we run it right after creating a predictor, we'll most probably get this output:

```sql
+----------+
| status   |
+----------+
| training |
+----------+
```

But if we wait a couple of minutes, this should be the output:

```sql
+----------+
| status   |
+----------+
| complete |
+----------+
```

Now, if the status of our predictor says `complete`, we can start making predictions!

## Making Predictions

### Making Predictions Via [`#!sql SELECT`](/sql/api/select)

You can make predictions by querying the predictor as if it were a table. The [`SELECT`](/sql/api/select/) syntax lets you make predictions for the label based on the chosen features.

```sql
SELECT rental_price, rental_price_explain
FROM mindsdb.home_rentals_model
WHERE sqft = 823 AND location='good' AND neighborhood='downtown' AND days_on_market=10;
```

On execution, you get the following output:

```sql
+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------+
| rental_price | rental_price_explain                                                                                                                          |
+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------+
| 4394         | {"predicted_value": 4394, "confidence": 0.99, "anomaly": null, "truth": null, "confidence_lower_bound": 4313, "confidence_upper_bound": 4475} |
+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------+
```

### Making Batch Predictions Via [`#!sql JOIN`](/sql/api/join)

Also, you can make bulk predictions by joining a table with your predictor.

```sql
SELECT t.rental_price as real_price, m.rental_price as predicted_price, t.number_of_rooms,  t.number_of_bathrooms, t.sqft, t.location, t.days_on_market 
FROM example_db.demo_data.home_rentals as t 
JOIN mindsdb.home_rentals_model as m
LIMIT 100;
```

On execution, you get the following output:

```sql
+------------+-----------------+-----------------+---------------------+------+----------+----------------+
| real_price | predicted_price | number_of_rooms | number_of_bathrooms | sqft | location | days_on_market |
+------------+-----------------+-----------------+---------------------+------+----------+----------------+
| 3901       | 3886            | 2               | 1                   | 917  | great    | 13             |
| 2042       | 2007            | 0               | 1                   | 194  | great    | 10             |
| 1871       | 1865            | 1               | 1                   | 543  | poor     | 18             |
| 3026       | 3020            | 2               | 1                   | 503  | good     | 10             |
| 4774       | 4748            | 3               | 2                   | 1066 | good     | 13             |
+------------+-----------------+-----------------+---------------------+------+----------+----------------+
```

## What's Next?

Have fun while trying it out yourself!

* Bookmark [MindsDB repository on GitHub](https://github.com/mindsdb/mindsdb).
* Sign up for a free [MindsDB account](https://cloud.mindsdb.com/register).
* Engage with the MindsDB community on [Slack](https://mindsdb.com/joincommunity) or [GitHub](https://github.com/mindsdb/mindsdb/discussions) to ask questions and share your ideas and thoughts.

If this tutorial was helpful, please give us a GitHub star [here](https://github.com/mindsdb/mindsdb).

# Forecasting Quarterly House Sales with MindsDB

## Introduction

In this tutorial, we'll create, train, and query a machine learning model, which, in MindsDB language, is an `AI Table` or a `predictor`. We aim to predict sales forecasts for houses using a multivariate time series strategy.

Make sure you have access to a working MindsDB installation either locally or via [cloud.mindsdb.com](https://cloud.mindsdb.com/).

You can learn how to set up your account at MindsDB Cloud by following [this guide](https://docs.mindsdb.com/setup/cloud/). Another way is to set up MindsDB locally using [Docker](https://docs.mindsdb.com/setup/self-hosted/docker/) or [Python](https://docs.mindsdb.com/setup/self-hosted/pip/source/).

Let's get started.

## The Data

### Connecting the data

There are a couple of ways you can get the data to follow through with this tutorial.

=== "Connecting as a database via `#!sql CREATE DATABASE`"

    You can connect to a demo database that we've prepared for you. It contains the data used throughout this tutorial which is the `#!sql example_db.demo_data.house_sales` table.

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
    FROM example_db.demo_data.house_sales 
    LIMIT 10;
    ```

=== "Connecting as a file"

    The dataset we use in this tutorial is the pre-processed version of the *House Property Sales* data. You can download it from [Kaggle](https://www.kaggle.com/datasets/htagholdings/property-sales). We use the *ma_lga_12345.csv* file.

    And [this guide](https://docs.mindsdb.com/sql/create/file/) explains how to upload a file to MindsDB.

    Now, you can query the uploaded file as if it were a table.

    ```sql
    SELECT *
    FROM files.house_sales
    LIMIT 10;
    ```

!!! Warning "From now on, we will use the `files.house_sales` file as a table. Make sure you replace it with `#!sql example_db.demo_data.house_sales` if you use the `demo` database."

### Understanding the Data

We will use the house sales dataset where each row represents one real estate (house or unit). It tracks quarterly moving averages of house sales aggregated by type and amount of bedrooms in each listing. These quarterly moving averages will be predicted in the following sections of this tutorial.

Below is the sample data stored in the house sales dataset.

```sql
+----------+------+-----+--------+
|saledate  |MA    |type |bedrooms|
+----------+------+-----+--------+
|30/09/2007|441854|house|2       |
|31/12/2007|441854|house|2       |
|31/03/2008|441854|house|2       |
|30/06/2016|430880|unit |2       |
|30/09/2016|430654|unit |2       |
+----------+------+-----+--------+
```

Where:

| Column                | Description                                                                                  | Data Type           | Usage   |
| :-------------------- | :------------------------------------------------------------------------------------------- | ------------------- | ------- |
| `saledate`            | The date of sale                                                                             | `date`              | Feature |
| `MA`                  | Moving average of the historical median price of the house or unit                           | `integer`           | Label   |
| `type`                | Type of property (either a house or a unit)                                                  | `character varying` | Feature |
| `bedrooms`            | Number of bedrooms                                                                           | `integer`           | Feature |

!!!Info "Labels and Features"

    A **label** is the thing we're predicting — the y variable in simple linear regression.
    A **feature** is an input variable — the x variable in simple linear regression.

## Training a Predictor Via [`#!sql CREATE PREDICTOR`](/sql/create/predictor)

Let's create and train your first machine learning predictor. For that, we are going to use the [`#!sql CREATE PREDICTOR`](/sql/create/predictor) syntax where we specify what sub-query to train `#!sql FROM` (features) and what we want to `#!sql PREDICT` (labels).

```sql
CREATE PREDICTOR mindsdb.house_sales_predictor
FROM files
  (SELECT * FROM house_sales)
PREDICT MA
ORDER BY saledate
GROUP BY bedrooms, type
-- as the target is quarterly,
-- we will look back two years to forecast the next one
WINDOW 8
HORIZON 4;
```

We use all of the columns as features, except for the `MA` column whose value is going to be predicted.

MindsDB makes it simple so that we don't need to repeat the predictor creation process for every group, that is, for every number of bedrooms or for every type of a real estate. Instead, we just group by both the `bedrooms` and `type` columns and the predictor learns from all series and enables forecasts for all of them!

## Checking the Status of a Predictor

A predictor may take a couple of minutes for the training to complete. You can monitor the status of your predictor by using this SQL command:

```sql
SELECT status
FROM mindsdb.predictors
WHERE name='house_sales_predictor';
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

You can make predictions by querying the predictor as if it were a table. The [`SELECT`](/sql/api/select/) syntax lets you make predictions for the label based on the chosen features for a given period of time. Usually, you want to know what happens right after the latest training data point that was fed. We have a special keyword for that: the `LATEST` keyword.

```sql
SELECT m.saledate AS date, m.MA AS forecast
FROM mindsdb.house_sales_predictor AS m 
JOIN files.house_sales AS t
WHERE t.saledate > LATEST 
AND t.type = 'house' 
AND t.bedrooms = 2
LIMIT 4;
```

Now, try changing the `type` column value to *unit*, or the `bedrooms` column value to any number between 1 to 5, and check how the forecasts vary. This is because MindsDB recognizes each grouping as being its own different time series.

## What's Next?

Have fun while trying it out yourself!

* Bookmark [MindsDB repository on GitHub](https://github.com/mindsdb/mindsdb).
* Sign up for a free [MindsDB account](https://cloud.mindsdb.com/register).
* Engage with the MindsDB community on [Slack](https://mindsdb.com/joincommunity) or [GitHub](https://github.com/mindsdb/mindsdb/discussions) to ask questions and share your ideas and thoughts.

If this tutorial was helpful, please give us a GitHub star [here](https://github.com/mindsdb/mindsdb).

# Forecast Quarterly House Sales using MindsDB

## Introduction

In this short example, we will produce forecasts for a multivariate time series.

Make sure you have access to a working MindsDB installation - either locally or via [cloud.mindsdb.com](https://cloud.mindsdb.com/).

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
    }
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
    ```

!!! Warning "From now on, we will use the `files.house_sales` file as a table. Make sure you replace it with `#!sql example_db.demo_data.house_sales` if you use the demo database."

### Understanding the Data

We will use the house sales dataset. It tracks quarterly moving averages of house sales aggregated by type and amount of bedrooms in each listing.

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

Now, we can specify that we want to forecast the `MA` column, which is a moving average of the historical median price of the house or unit. However, looking at the data you can see several entries for the same date. It depends on two factors: 1. how many bedrooms the properties have, and 2. whether properties are "houses" or "units". This means that we can have up to ten different groupings here. Although, if you do some digging, you will find we only have seven of the possible ten combinations.

MindsDB makes it simple so that we don't need to repeat the predictor creation process for every group there is. Instead, we can just group for both columns and the predictor will learn from all series and enable forecasts for all of them! Here is the SQL command to do so:

```
CREATE PREDICTOR 
  mindsdb.home_sales_model
FROM files
  (SELECT * FROM house_sales)
PREDICT MA
ORDER BY saledate
GROUP BY bedrooms, type
-- as the target is quarterly, we will look back two years to forecast the next one
WINDOW 8
HORIZON 4;  
```

## Checking the Status of a Predictor

You can check the status of the predictor by executing the folowing SQL command:

```
SELECT * FROM mindsdb.predictors where name='home_sales_model';
```

## Making Predictions

Once the predictor has been successfully trained, you can query it to get forecasts for a given period of time. Usually, you'll want to know what happens right after the latest training data point that was fed. We have a special bit of syntax for that: the `LATEST` keyword.

```
SELECT m.saledate as date,
       m.MA as forecast
FROM mindsdb.home_sales_model as m 
JOIN files.house_sales as t
WHERE t.saledate > LATEST AND t.type = 'house' AND t.bedrooms = 2
LIMIT 4;
```

Now, try changing the `type` column value to *unit*, or the `bedrooms` column value to any number between 1 to 5, and check how the forecast varies. This is because MindsDB recognizes each grouping as being its own different time series.

## What's Next?

Have fun while trying it out yourself!

* Bookmark [MindsDB repository on GitHub](https://github.com/mindsdb/mindsdb).
* Sign up for a free [MindsDB account](https://cloud.mindsdb.com/register).
* Engage with the MindsDB community on [Slack](https://mindsdb.com/joincommunity) or [GitHub](https://github.com/mindsdb/mindsdb/discussions) to ask questions and share your ideas and thoughts.

If this tutorial was helpful, please give us a GitHub star [here](https://github.com/mindsdb/mindsdb).

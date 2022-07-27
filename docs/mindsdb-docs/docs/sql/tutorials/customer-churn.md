# Predicting Customer Churn with MindsDB

## Introduction

In this tutorial, we'll create, train, and query a machine learning model, which, in MindsDB language, is an `AI Table` or a `predictor`. We aim to predict the probability of churn for new customers of a telecom company.

Make sure you have access to a working MindsDB installation either locally or via [cloud.mindsdb.com](https://cloud.mindsdb.com/).

You can learn how to set up your account at MindsDB Cloud by following [this guide](https://docs.mindsdb.com/setup/cloud/). Another way is to set up MindsDB locally using [Docker](https://docs.mindsdb.com/setup/self-hosted/docker/) or [Python](https://docs.mindsdb.com/setup/self-hosted/pip/source/).

Let's get started.

## The Data

### Connecting the data

There are a couple of ways you can get the data to follow through with this tutorial.

=== "Connecting as a database via `#!sql CREATE DATABASE`"

    You can connect to a demo database that we've prepared for you. It contains the data used throughout this tutorial which is the `#!sql example_db.demo_data.customer_churn` table.

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
    FROM example_db.demo_data.customer_churn 
    LIMIT 10;
    ```

=== "Connecting as a file"

    In this tutorial, we use the customer churn dataset. You can download it [here](https://github.com/mindsdb/mindsdb-examples/blob/master/classics/customer_churn/raw_data/WA_Fn-UseC_-Telco-Customer-Churn.csv).

    And [this guide](https://docs.mindsdb.com/sql/create/file/) explains how to upload a file to MindsDB.

    Now, you can query the uploaded file as if it were a table.

    ```sql
    SELECT *
    FROM files.churn
    LIMIT 10;
    ```

!!! Warning "From now on, we will use the `files.churn` file as a table. Make sure you replace it with `#!sql example_db.demo_data.customer_churn` if you use the `demo` database."

### Understanding the Data

We will use the customer churn dataset where each row represents one customer. In the following sections of this tutorial, we will predict if the customer is going to stop using the company products.

Below is the sample data stored in the customer churn dataset.

```sql
+----------+------+-------------+-------+----------+------+------------+----------------+---------------+--------------+------------+----------------+-----------+-----------+---------------+--------------+----------------+-------------------------+--------------+------------+-----+
|customerID|gender|SeniorCitizen|Partner|Dependents|tenure|PhoneService|MultipleLines   |InternetService|OnlineSecurity|OnlineBackup|DeviceProtection|TechSupport|StreamingTV|StreamingMovies|Contract      |PaperlessBilling|PaymentMethod            |MonthlyCharges|TotalCharges|Churn|
+----------+------+-------------+-------+----------+------+------------+----------------+---------------+--------------+------------+----------------+-----------+-----------+---------------+--------------+----------------+-------------------------+--------------+------------+-----+
|7590-VHVEG|Female|0            |Yes    |No        |1     |No          |No phone service|DSL            |No            |Yes         |No              |No         |No         |No             |Month-to-month|Yes             |Electronic check         |29.85         |29.85       |No   |
|5575-GNVDE|Male  |0            |No     |No        |34    |Yes         |No              |DSL            |Yes           |No          |Yes             |No         |No         |No             |One year      |No              |Mailed check             |56.95         |1889.5      |No   |
|3668-QPYBK|Male  |0            |No     |No        |2     |Yes         |No              |DSL            |Yes           |Yes         |No              |No         |No         |No             |Month-to-month|Yes             |Mailed check             |53.85         |108.15      |Yes  |
|7795-CFOCW|Male  |0            |No     |No        |45    |No          |No phone service|DSL            |Yes           |No          |Yes             |Yes        |No         |No             |One year      |No              |Bank transfer (automatic)|42.3          |1840.75     |No   |
|9237-HQITU|Female|0            |No     |No        |2     |Yes         |No              |Fiber optic    |No            |No          |No              |No         |No         |No             |Month-to-month|Yes             |Electronic check         |70.7          |151.65      |Yes  |
+----------+------+-------------+-------+----------+------+------------+----------------+---------------+--------------+------------+----------------+-----------+-----------+---------------+--------------+----------------+-------------------------+--------------+------------+-----+
```

Where:

| Column                | Description                                                                                                      | Data Type           | Usage   |
| :-------------------- | :--------------------------------------------------------------------------------------------------------------- | ------------------- | ------- |
| `CustomerId`          | The identification number per customer                                                                           | `character varying` | Feature |
| `Gender`              | The gender of a customer                                                                                         | `character varying` | Feature |
| `SeniorCitizen`       | It indicates whether the customer is a senior citizen (1) or not (0)                                             | `integer`           | Feature |
| `Partner`             | It indicates whether the customer has a partner (Yes) or not (No)                                                | `character varying` | Feature |
| `Dependents`          | It indicates whether the customer has dependents (Yes) or not (No)                                               | `character varying` | Feature |
| `Tenure`              | Number of months the customer has stayed with the company                                                        | `integer`           | Feature |
| `PhoneService`        | It indicates whether the customer has a phone service (Yes) or not (No)                                          | `character varying` | Feature |
| `MultipleLines`       | It indicates whether the customer has multiple lines (Yes) or not (No, No phone service)                         | `character varying` | Feature |
| `InternetService`     | Customer’s internet service provider (DSL, Fiber optic, No)                                                      | `character varying` | Feature |
| `OnlineSecurity`      | It indicates whether the customer has online security (Yes) or not (No, No internet service)                     | `character varying` | Feature |
| `OnlineBackup`        | It indicates whether the customer has online backup (Yes) or not (No, No internet service)                       | `character varying` | Feature |
| `DeviceProtection`    | It indicates whether the customer has device protection (Yes) or not (No, No internet service)                   | `character varying` | Feature |
| `TechSupport`         | It indicates whether the customer has tech support (Yes) or not (No, No internet service)                        | `character varying` | Feature |
| `StreamingTv`         | It indicates whether the customer has streaming TV (Yes) or not (No, No internet service)                        | `character varying` | Feature |
| `StreamingMovies`     | It indicates whether the customer has streaming movies (Yes) or not (No, No internet service)                    | `character varying` | Feature |
| `Contract`            | The contract term of the customer (Month-to-month, One year, Two year)                                           | `character varying` | Feature |
| `PaperlessBilling`    | It indicates whether the customer has paperless billinig (Yes) or not (No)                                       | `character varying` | Feature |
| `PaymentMethod`       | Customer’s payment method (Electronic check, Mailed check, Bank transfer (automatic), Credit card (automatic))   | `character varying` | Feature |
| `MonthlyCharges`      | The monthly charge amount                                                                                        | `money`             | Feature |
| `TotalCharges`        | The total amount charged to the customer                                                                         | `money`             | Feature |
| `Churn`               | It indicates whether the customer churned (Yes) or not (No)                                                      | `character varying` | Label   |

!!!Info "Labels and Features"

    A **label** is the thing we're predicting — the y variable in simple linear regression.
    A **feature** is an input variable — the x variable in simple linear regression.

## Training a Predictor Via [`#!sql CREATE PREDICTOR`](/sql/create/predictor)

Let's create and train your first machine learning predictor. For that, we are going to use the [`#!sql CREATE PREDICTOR`](/sql/create/predictor) syntax where we specify what sub-query to train `#!sql FROM` (features) and what we want to `#!sql PREDICT` (labels).

```sql
CREATE PREDICTOR mindsdb.customer_churn_predictor
FROM files
  (SELECT * FROM churn)
PREDICT Churn;
```

We use all of the columns as features, except for the `Churn` column whose value is going to be predicted.

## Checking the Status of a Predictor

A predictor may take a couple of minutes for the training to complete. You can monitor the status of your predictor by using this SQL command:

```sql
SELECT status
FROM mindsdb.predictors
WHERE name='customer_churn_predictor';
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

You can make predictions by querying the predictor as if it were a table. The [`SELECT`](/sql/api/select/) syntax lets you make predictions for the label based on the chosen features.

```sql
SELECT Churn, Churn_confidence, Churn_explain
FROM mindsdb.customer_churn_predictor
WHERE SeniorCitizen=0 
AND Partner='Yes' 
AND Dependents='No' 
AND tenure=1 
AND PhoneService='No' 
AND MultipleLines='No phone service' 
AND InternetService='DSL';
```

On execution, you get the following output:

```sql
+-------+---------------------+-------------------------------------------------------------------------------------------------+
| Churn | Churn_confidence    | Churn_explain                                                                                   |
+-------+---------------------+-------------------------------------------------------------------------------------------------+
| Yes   | 0.7865168539325843  | {"predicted_value": "Yes", "confidence": 0.7865168539325843, "anomaly": null, "truth": null}    |
+-------+---------------------+-------------------------------------------------------------------------------------------------+
```

Let's try another prediction.

An important thing to check is the `important_missing_information` value, where MindsDB points to the important missing information that should be included to give a more accurate prediction. In this case, the `Contract`, `MonthlyCharges`, `TotalCharges`, and `OnlineBackup` columns are the important missing information. Let’s include those values in the `WHERE` clause, and run a new query.

```sql
SELECT Churn, Churn_confidence, Churn_explain
FROM mindsdb.customer_churn_predictor
WHERE SeniorCitizen=0 
AND Partner='Yes' 
AND Dependents='No' 
AND tenure=1 
AND PhoneService='No' 
AND MultipleLines='No phone service' 
AND InternetService='DSL' 
AND OnlineSecurity='No' 
AND OnlineBackup='Yes' 
AND DeviceProtection='No' 
AND TechSupport='No' 
AND StreamingTV='No' 
AND StreamingMovies='No' 
AND Contract='Month-to-month' 
AND PaperlessBilling='Yes' 
AND PaymentMethod='Electronic check' 
AND MonthlyCharges=29.85 
AND TotalCharges=29.85;
```

On execution, you get the following output:

```sql
+-------+---------------------+-------------------------------------------------------------------------------------------------+
| Churn | Churn_confidence    | Churn_explain                                                                                   |
+-------+---------------------+-------------------------------------------------------------------------------------------------+
| Yes   | 0.8202247191011236  | {"predicted_value": "Yes", "confidence": 0.8202247191011236, "anomaly": null, "truth": null}    |
+-------+---------------------+-------------------------------------------------------------------------------------------------+
```

Here, MindsDB predicted the probability of this customer churning with the confidence of around 82%.

## What's Next?

Have fun while trying it out yourself!

* Bookmark [MindsDB repository on GitHub](https://github.com/mindsdb/mindsdb).
* Sign up for a free [MindsDB account](https://cloud.mindsdb.com/register).
* Engage with the MindsDB community on [Slack](https://mindsdb.com/joincommunity) or [GitHub](https://github.com/mindsdb/mindsdb/discussions) to ask questions and share your ideas and thoughts.

If this tutorial was helpful, please give us a GitHub star [here](https://github.com/mindsdb/mindsdb).

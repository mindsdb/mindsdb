## Introduction

MindsDB as a Machine Learning framework can help marketing, sales, and customer retention teams determine the best incentive and the right time to make an offer to minimize customer turnover.

In this tutorial you will learn how to use SQL queries to train a machine learning model and make predictions in three simple steps:

1. Connect a database with customer's data to MindsDB.
2. Use an `CREATE PREDICTOR` statement to train the machine learning model automatically.
3. Query predictions with a simple `SELECT` statement from MindsDB `AI Table` (this special table returns data from an ML model upon being queried).

Using SQL to perform machine learning at the data layer will bring you many benefits like removing unnecessary ETL-ing, seamless integration with your data, and enabling predictive analytics in your BI tool.  Let's see how this works with a real world example to predict the probability of churn for a new customer of a telecom company.

> Note: You can follow up this tutorial by connecting to your own database and using different data - the same workflow applies to most machine learning use cases.

### Pre-requisites

First, log into MindsDB Cloud or make sure you have successfully installed MindsDB. Check out the installation guide for [Docker](https://docs.mindsdb.com/setup/self-hosted/docker/) or [PyPi](https://docs.mindsdb.com/setup/self-hosted/pip/source/). Then, you will need mysql-client, DBeaver, MySQL WOrkbench, etc. installed locally to connect to MySQL API.

### Log into Mindsdb Cloud

The first step is to log in to [Mindsdb Cloud](https://docs.mindsdb.com/deployment/cloud/) where we will gain access to the MySQL editor to execute SQL syntax. The Upload File feature and the MySQL Editor make it easy to upload a file, create a predictor, and make a prediction.

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
    }
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
    ```

!!! Warning "From now on, we will use the `files.churn` file as a table. Make sure you replace it with `#!sql example_db.demo_data.customer_churn` if you use the demo database."

### Understanding the Data

We will use the customer churn dataset where each row represents one customer. We will train a machine learning model to help us predict if the customer is going to stop using the company products.

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

We will create a machine learning model with the relevant parameters in order for it to train.

For that we are going to use the CREATE PREDICTOR syntax, where we specify what query to train FROM and what we want to learn to PREDICT:

```sql
CREATE PREDICTOR predictor_name
FROM files
  (SELECT * FROM file_name)
PREDICT column_name;
```

The required values that we need to provide are:

- predictor_name (string) - The name of the model
- file_name - The name of the table/file you have uploaded.
- column_name (string) - The feature you want to predict.

To train the model that will predict customer churn, run the following syntax:

```sql
CREATE PREDICTOR customer_churn
FROM files
  (SELECT * FROM churn)
PREDICT Churn;
```

![Create predictor](/assets/sql/tutorials/customer_churn/create_churn.png)

What we did here was to create a predictor called customer_churn to predict the Churn and also ignore the gender column as an irrelevant column for the model.The model training has started.

## Checking the Status of a Predictor

You can check the predictors status and will be able to make a prediction once the status shows complete:

```sql
SELECT * FROM mindsdb.predictors where name='customer_churn';
```

![select](/assets/sql/tutorials/customer_churn/select.png)

## Making Predictions

The next steps would be to query the model and predict the customer churn. Let’s be creative and imagine a customer. Customer will use only DSL service, no phone service and multiple lines, she was with the company for 1 month and has a partner. Add all of this information to the WHERE clause.

```sql
SELECT Churn, Churn_confidence, Churn_explain FROM mindsdb.customer_churn WHERE SeniorCitizen=0 AND Partner='Yes' 
AND Dependents='No' AND tenure=1 AND PhoneService='No' AND MultipleLines='No phone service' AND InternetService='DSL';
```

![customer churn](/assets/sql/tutorials/customer_churn/customer_churn.png)

With the confidence of around 82% MindsDB predicted that this customer will churn. One important thing to check here is the important_missing_information value, where MindsDB is pointing to the important missing information for giving a more accurate prediction, in this case, Contract, MonthlyCharges, TotalCharges and OnlineBackup. Let’s include those values in the WHERE clause, and run a new query:

```sql
SELECT Churn, Churn_confidence, Churn_explain FROM mindsdb.customer_churn WHERE SeniorCitizen=0 AND Partner='Yes' 
AND Dependents='No' AND tenure=1 AND PhoneService='No' AND MultipleLines='No phone service' AND InternetService='DSL' AND OnlineSecurity='No' AND OnlineBackup='Yes' AND DeviceProtection='No' AND TechSupport='No' AND StreamingTV='No' AND StreamingMovies='No' AND Contract='Month-to-month' AND PaperlessBilling='Yes' AND PaymentMethod='Electronic check' AND MonthlyCharges=29.85 AND TotalCharges=29.85;
```

![customer churn2](/assets/sql/tutorials/customer_churn/customer_churn2.png)

## What's Next?

Have fun while trying it out yourself!

* Bookmark [MindsDB repository on GitHub](https://github.com/mindsdb/mindsdb).
* Sign up for a free [MindsDB account](https://cloud.mindsdb.com/register).
* Engage with the MindsDB community on [Slack](https://mindsdb.com/joincommunity) or [GitHub](https://github.com/mindsdb/mindsdb/discussions) to ask questions and share your ideas and thoughts.

If this tutorial was helpful, please give us a GitHub star [here](https://github.com/mindsdb/mindsdb).

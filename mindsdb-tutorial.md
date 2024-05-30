# MindsDB Tutorial: Predicting Customer Churn

## Introduction
Welcome to the MindsDB tutorial! In this guide, we will walk you through the process of using MindsDB to predict customer churn using the "Telco Customer Churn" dataset. MindsDB is an open-source automated machine learning framework that simplifies the process of building and deploying machine learning models.

## Data Setup
The dataset we will use is the "Telco Customer Churn" dataset, which contains information about customer demographics, account information, and service usage. You can download the dataset from [Kaggle](https://www.kaggle.com/blastchar/telco-customer-churn).

## Connecting the Data
To follow this tutorial, you need to connect the dataset to MindsDB. If you are using MindsDB Cloud, you can upload the dataset directly. For local installations, you can connect to the dataset using a local path or a database connection.

```sql
-- Example SQL to connect the dataset in MindsDB
CREATE TABLE telco_customer_churn
FROM FILE '/path/to/telco_customer_churn.csv'
WITH
    FORMAT = 'csv',
    HEADER = TRUE;
```
## Understanding the Data

Before we build a model, let's understand the dataset. The "Telco Customer Churn" dataset includes features such as `gender`, `SeniorCitizen`, `Partner`, `Dependents`, `tenure`, `PhoneService`, `MultipleLines`, `InternetService`, `OnlineSecurity`, `OnlineBackup`, `DeviceProtection`, `TechSupport`, `StreamingTV`, `StreamingMovies`, `Contract`, `PaperlessBilling`, `PaymentMethod`, `MonthlyCharges`, and `TotalCharges`. Our target variable is `Churn`.

## Training a Predictor

To train a predictive model, we use the `CREATE MODEL` command. This command will train a model to predict whether a customer will churn based on the other features.

```sql
CREATE MODEL churn_predictor
FROM telco_customer_churn
PREDICT Churn
USING
    gender,
    SeniorCitizen,
    Partner,
    Dependents,
    tenure,
    PhoneService,
    MultipleLines,
    InternetService,
    OnlineSecurity,
    OnlineBackup,
    DeviceProtection,
    TechSupport,
    StreamingTV,
    StreamingMovies,
    Contract,
    PaperlessBilling,
    PaymentMethod,
    MonthlyCharges,
    TotalCharges;
```
If the status is `complete`, you can proceed to the next steps.

## Status of a Predictor

Once you have created the model, you can check its status to ensure it has been trained successfully.
```sql
SELECT status
FROM mindsdb.models
WHERE name='churn_predictor';
```
## Making Predictions

To make predictions with your trained model, use the `SELECT` statement.
```sql
SELECT Churn
FROM mindsdb.churn_predictor
WHERE gender = 'Female'
AND SeniorCitizen = 0
AND Partner = 'Yes'
AND Dependents = 'No'
AND tenure = 1
AND PhoneService = 'No'
AND MultipleLines = 'No phone service'
AND InternetService = 'DSL'
AND OnlineSecurity = 'No'
AND OnlineBackup = 'Yes'
AND DeviceProtection = 'No'
AND TechSupport = 'No'
AND StreamingTV = 'No'
AND StreamingMovies = 'No'
AND Contract = 'Month-to-month'
AND PaperlessBilling = 'Yes'
AND PaymentMethod = 'Electronic check'
AND MonthlyCharges = 29.85
AND TotalCharges = 29.85;
```
## Making a Single Prediction

You can also make a single prediction by specifying the feature values directly.
```sql
SELECT Churn
FROM mindsdb.churn_predictor
WHERE gender = 'Male'
AND SeniorCitizen = 1
AND Partner = 'No'
AND Dependents = 'No'
AND tenure = 5
AND PhoneService = 'Yes'
AND MultipleLines = 'No'
AND InternetService = 'Fiber optic'
AND OnlineSecurity = 'No'
AND OnlineBackup = 'No'
AND DeviceProtection = 'No'
AND TechSupport = 'No'
AND StreamingTV = 'No'
AND StreamingMovies = 'No'
AND Contract = 'One year'
AND PaperlessBilling = 'No'
AND PaymentMethod = 'Mailed check'
AND MonthlyCharges = 70.35
AND TotalCharges = 350.5;
```
## Making Batch Predictions

To make batch predictions, you can join the model with a dataset containing multiple rows of feature values.
```sql
SELECT d.*, p.Churn
FROM telco_customer_churn AS d
JOIN mindsdb.churn_predictor AS p
WHERE p.gender = d.gender
AND p.SeniorCitizen = d.SeniorCitizen
AND p.Partner = d.Partner
AND p.Dependents = d.Dependents
AND p.tenure = d.tenure
AND p.PhoneService = d.PhoneService
AND p.MultipleLines = d.MultipleLines
AND p.InternetService = d.InternetService
AND p.OnlineSecurity = d.OnlineSecurity
AND p.OnlineBackup = d.OnlineBackup
AND p.DeviceProtection = d.DeviceProtection
AND p.TechSupport = d.TechSupport
AND p.StreamingTV = d.StreamingTV
AND p.StreamingMovies = d.StreamingMovies
AND p.Contract = d.Contract
AND p.PaperlessBilling = d.PaperlessBilling
AND p.PaymentMethod = d.PaymentMethod
AND p.MonthlyCharges = d.MonthlyCharges
AND p.TotalCharges = d.TotalCharges;
```

This tutorial introduces a new topic—predicting customer churn—using the "Telco Customer Churn" dataset. It follows the structure required by MindsDB, ensuring that all necessary chapters are included. Feel free to modify or expand the content as needed for your specific use case.

## What's Next?

We hope you found this tutorial helpful! To learn more about MindsDB and explore additional features, check out the following resources:
-   [MindsDB](https://mindsdb.com/)
-   [MindsDB Documentation](https://docs.mindsdb.com/)
-   [Join the MindsDB Community on Slack](https://mindsdb.com/joincommunity)
-   [MindsDB GitHub Repository](https://github.com/mindsdb/mindsdb/)
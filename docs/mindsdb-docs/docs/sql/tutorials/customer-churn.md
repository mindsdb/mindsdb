
MindsDB as a Machine Learning framework can help marketing, sales, and customer retention teams determine the best incentive and the right time to make an offer to minimize customer turnover.

In this tutorial you will learn how to use SQL queries to train a machine learning model and make predictions in three simple steps:

1. Connect a database with customer's data to MindsDB.
2. Use an `CREATE PREDICTOR` statement to train the machine learning model automatically.
3. Query predictions with a simple `SELECT` statement from MindsDB `AI Table` (this special table returns data from an ML model upon being queried).

Using SQL to perform machine learning at the data layer will bring you many benefits like removing unnecessary ETL-ing, seamless integration with your data, and enabling predictive analytics in your BI tool.  Let's see how this works with a real world example to predict the probability of churn for a new customer of a telecom company.

> Note: You can follow up this tutorial by connecting to your own database and using different data - the same workflow applies to most machine learning use cases.

## Pre-requisites

First, you can log into MindsDB Cloud or make sure you have successfully installed MindsDB. Check out the installation guide for [Docker](/deployment/docker/) or [PyPi](/deployment/source/) install. Second, you will need to have mysql-client or DBeaver, MySQL WOrkbench etc installed locally to connect to MySQL API.

## Log into Mindsdb Cloud

The first step will be to log into [Mindsdb Cloud](https://docs.mindsdb.com/deployment/cloud/) , where we will gain access to the MySQL editor to execute SQL syntax. The Upload file feature and the MySQL Editor will make it easy to upload a file, create a predictor and make a prediction.

### Upload a data file to MindsDB Cloud

The datafile can be uploaded directly to Mindsdb Cloud for you to query. 

1. Navigate to **+Upload file** and select the option.
2. Select **Upload file** to upsert a file from your local files or drag the file onto the 'Upload a file' section.
3. Once the file has been uploaded,name the file you as a table name.
4. Save and continue.

The file will be stored as a table in your files table.

![Upload file](/assets/sql/tutorials/customer_churn/upload.png)

You can query the file you have uploaded as a table, eg.

```sql
SELECT * FROM files.churn
```

### Data Overview

In this tutorial, we will use the customer churn data-set . Each row represents a customer and we will train a machine learning model to help us predict if the customer is going to stop using the company products. Below is a short description of each feature inside the data.

- CustomerId - Customer ID
- Gender - Male or Female customer
-  SeniorCitizen - Whether the customer is a senior citizen or not (1, 0)
-  Partner - Whether the customer has a partner or not (Yes, No)
-  Dependents - Whether the customer has dependents or not (Yes, No)
-  Tenure - Number of months the customer has stayed with the company
-  PhoneService - Whether the customer has a phone service or not (Yes, No)
-  MultipleLines - Whether the customer has multiple lines or not (Yes, No, No phone service)
-  InternetService - Customer’s internet service provider (DSL, Fiber optic, No)
-  OnlineSecurity - Whether the customer has online security or not (Yes, No, No internet service)
-  OnlineBackup - Whether the customer has online backup or not (Yes, No, No internet service)
-  DeviceProtection - Whether the customer has device protection or not (Yes, No, No internet service)
-  TechSupport - Whether the customer has tech support or not (Yes, No, No internet service)
-  StreamingTv - Whether the customer has streaming TV or not (Yes, No, No internet service)
-  StreamingMovies - Whether the customer has streaming movies or not (Yes, No, No internet service)
-  Contract - The contract term of the customer (Month-to-month, One year, Two year)
-  PaperlessBilling - Whether the customer has paperless billing or not (Yes, No)
-  PaymentMethod - The customer’s payment method (Electronic check, Mailed check, Bank transfer (automatic), Credit card (automatic))
-  MonthlyCharges - The monthly charge amount
-  TotalCharges - The total amount charged to the customer
-  Churn - Whether the customer churned or not (Yes or No). This is what we want to predict.


### Create a predictor

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

You can check the predictors status and will be able to make a prediction once the status shows complete:

```sql
SELECT * FROM mindsdb.predictors where name='customer_churn';
```

![select](/assets/sql/tutorials/customer_churn/select.png)

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


MindsDB as a Machine Learning framework can help marketing, sales, and customer retention teams determine the best incentive and the right time to make an offer to minimize customer turnover.

In this tutorial you will learn how to use SQL queries to train a machine learning model and make predictions in three simple steps:

1. Connect a database with customer's data to MindsDB.
2. Use an `CREATE PREDICTOR` statement to train the machine learning model automatically.
3. Query predictions with a simple `SELECT` statement from MindsDB `AI Table` (this special table returns data from an ML model upon being queried).

Using SQL to perform machine learning at the data layer will bring you many benefits like removing unnecessary ETL-ing, seamless integration with your data, and enabling predictive analytics in your BI tool.  Let's see how this works with a real world example to predict the probability of churn for a new customer of a telecom company.

> Note: You can follow up this tutorial by connecting to your own database and using different data - the same workflow applies to most machine learning use cases.

## Pre-requisites

First, make sure you have successfully installed MindsDB. Check out the installation guide for [Docker](/deployment/docker/) or [PyPi](/deployment/source/) install. Second, you will need to have mysql-client or DBeaver, MySQL WOrkbench etc installed locally to connect to MySQL API.

## Database Connection

First, we need to connect MindsDB to the database where the Customer Churn data is stored. In the left navigation click on Database. Next, click on the ADD DATABASE. Here, we need to provide all of the required parameters for connecting to the database.

* Supported Database - select the database that you want to connect to
* Integrations Name - add a name to the integration
* Database - the database name
* Host - database host name
* Port - database port
* Username - database user
* Password - user's password

![Connect to DB](/assets/sql/tutorials/connect.gif)

Then, click on CONNECT. The next step is to use the MySQL client to connect to MindsDB’s MySQL API and train a new model that shall predict customer churn.

## Connect to MindsDB’s MySQL API

I will use a mysql command line client in the next part of the tutorial but you can follow up with the one that works the best for you, like Dbeaver. The first step is to use the MindsDB Cloud user to connect to the MySQL API:

```
mysql -h cloud.mindsdb.com --port 3306 -u theusername@mail.com -p
```

In the above command, we specify the hostname and user name explicitly, as well as a password for connecting.


![Connect mysql-client](/assets/sql/tutorials/connect.png)

If you got the above screen that means you have successfully connected. If you have an authentication error, please make sure you are providing the email address you have used to create an account on MindsDB Cloud.

### Data Overview

In this tutorial, we will use the [customer churn data-set](https://raw.githubusercontent.com/mindsdb/mindsdb-examples/master/classics/customer_churn/raw_data/WA_Fn-UseC_-Telco-Customer-Churn.csv) . Each row represents a customer and we will train a machine learning model to help us predict if the customer is going to stop using the company products. Below is a short description of each feature inside the data.

* CustomerId - Customer ID
* Gender - Male or Female customer
* SeniorCitizen - Whether the customer is a senior citizen or not (1, 0)
* Partner - Whether the customer has a partner or not (Yes, No)
* Dependents - Whether the customer has dependents or not (Yes, No)
* Tenure - Number of months the customer has stayed with the company
* PhoneService - Whether the customer has a phone service or not (Yes, No)
* MultipleLines - Whether the customer has multiple lines or not (Yes, No, No phone service)
* InternetService - Customer’s internet service provider (DSL, Fiber optic, No)
* OnlineSecurity - Whether the customer has online security or not (Yes, No, No internet service)
* OnlineBackup - Whether the customer has online backup or not (Yes, No, No internet service)
* DeviceProtection - Whether the customer has device protection or not (Yes, No, No internet service)
* TechSupport - Whether the customer has tech support or not (Yes, No, No internet service)
* StreamingTv - Whether the customer has streaming TV or not (Yes, No, No internet service)
* StreamingMovies - Whether the customer has streaming movies or not (Yes, No, No internet service)
* Contract - The contract term of the customer (Month-to-month, One year, Two year)
* PaperlessBilling - Whether the customer has paperless billing or not (Yes, No)
* PaymentMethod - The customer’s payment method (Electronic check, Mailed check, Bank transfer (automatic), Credit card (automatic))
* MonthlyCharges - The monthly charge amount
* TotalCharges - The total amount charged to the customer
* Churn - Whether the customer churned or not (Yes or No). This is what we want to predict.

## Using SQL Statements to train/query models

Now, we will train a new machine learning model from the datasource we have created using MindsDB Studio. 
Switch back to mysql-client and run:

```
use mindsdb;
show tables;
```

![use  mindsdb](/assets/sql/tutorials/use.png)

You will notice there are 2 tables available inside the MindsDB database. To train a new machine learning model we will need to CREATE PREDICTOR as a new record inside the predictors table as:

```sql
CREATE PREDICTOR mindsdb.predictor_name
FROM integration_name 
(SELECT column_name, column_name2 FROM table_name)
PREDICT column_name as column_alias;
```

The required values that we need to provide are:

* predictor_name (string) - The name of the model
* integration_name (string) - The name of connection to your database.
* column_name (string) - The feature you want to predict.

To train the model that will predict customer churn run:

```sql
CREATE PREDICTOR mindsdb.churn_model FROM demo (SELECT * FROM CustomerChurnData)
PREDICT Churn as customer_churn USING {"ignore_columns": ["gender"]};
```

![INSERT query](/assets/sql/tutorials/insert.png)

What we did here was to create a predictor called `customer_churn `to predict the `Churn` and also ignore the `gender` column as an irrelevant column for the model. Also note that the ID columns in this case `customerId` will be automatically detected by MindsDB and ignored. The model training has started. To check if the training has finished you can SELECT the model name from the predictors table:

```sql
SELECT * FROM mindsdb.predictors WHERE name='churn_model';
```

The complete status means that the model training has successfully finished. 

![SELECT status](/assets/sql/tutorials/status.png)

The next steps would be to query the model and predict the customer churn. Let’s be creative and imagine a customer. Customer will use only DSL service, no phone service and multiple lines, she was with the company for 1 month and has a partner. Add all of this information to the `WHERE` clause.

```sql
SELECT Churn, Churn_confidence, Churn_explain as Info  FROM mindsdb.churn_model WHERE when_data='{"SeniorCitizen": 0, "Partner": "Yes", "Dependents": "No", "tenure": 1, "PhoneService": "No", "MultipleLines": "No phone service", "InternetService": "DSL"}';
```

![SELECT from model](/assets/sql/tutorials/select.png)

With the confidence of around 82% MindsDB predicted that this customer will churn. One important thing to check here is the important_missing_information value, where MindsDB is pointing to the important missing information for giving a more accurate prediction, in this case, Contract, MonthlyCharges, TotalCharges and OnlineBackup. Let’s include those values in the WHERE clause, and run a new query:

```sql
SELECT Churn, Churn_confidence, Churn_explain as Info  FROM mindsdb.churn_model WHERE when_data='{"SeniorCitizen": 0, "Partner": "Yes", "Dependents": "No", "tenure": 1, "PhoneService": "No", "MultipleLines": "No phone service", "InternetService": "DSL", "OnlineSecurity": "No", "OnlineBackup": "Yes", "DeviceProtection": "No", "TechSupport": "No", "StreamingTV": "No", "StreamingMovies": "No", "Contract": "Month-to-month", "PaperlessBilling": "Yes", "PaymentMethod": "Electronic check", "MonthlyCharges": 29.85, "TotalCharges": 29.85}';
```

![SELECT from model info](/assets/sql/tutorials/selecti.png)

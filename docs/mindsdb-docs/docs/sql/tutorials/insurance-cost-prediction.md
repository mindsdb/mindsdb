
# Predict insurance cost using Mindsdb

*Level: Easy*
*Dataset: [Medical Cost Personal Data](https://www.kaggle.com/mirichoi0218/insurance)*

Can you accurately predict insurance costs?  
In this tutorial, you will learn how to predict insurance costs using Mindsdb.
This tutorial is very easy because you don't need to learn any machine learning algorithm, all you need to know is just SQL and Mindsdb.

## Pre-requisites

First, you need Mindsdb, if you want to use Mindsdb locally, you need to install Mindsdb using
[Docker](https://docs.mindsdb.com/deployment/docker/) or [Python](https://docs.mindsdb.com/deployment/pypi/). 
But if you want to use Mindsdb without installing it locally, you can use [Cloud Mindsdb](https://cloud.mindsdb.com/signup). 
And in this tutorial, I'm using Cloud Mindsdb.

Second, you need a MySQL client to connect to Mindsdb MYSQL API.

## Connect your database

First, you need to connect MindsDB to the database where the dataset is stored.
In the left navigation click on Database, click on the ADD DATABASE.
And you need to provide all of the required parameters for connecting to the database.

![](https://github.com/kinkusuma/mindsdb/blob/add-regression-tutorial-sql/docs/mindsdb-docs/docs/assets/sql/tutorials/insurance-cost-prediction/add-database-cloud-mindsdb-sql.png)

* Supported Database - select the database that you want to connect to
* Integrations Name - add a name to the integration, here I'm using 'mysql' but you can name it anything
* Database - the database name
* Host - database hostname
* Port - database port
* Username - database user
* Password - user's password

Then, click on CONNECT.  
The next step is to use the MySQL client to connect to MindsDB’s MySQL API, train a new model, and make a prediction.

## Connect to MindsDB’s MySQL API

Here I'm using MySQL command-line client, but you can also follow up with the one that works the best for you, like Dbeaver.  
The first step is to use the MindsDB Cloud user to connect to the Mindsdb MySQL API, using this command:

![](https://github.com/kinkusuma/mindsdb/blob/add-regression-tutorial-sql/docs/mindsdb-docs/docs/assets/sql/tutorials/insurance-cost-prediction/connect-mindsdb-sql.png)

You need to specify the hostname and user name explicitly, as well as a password for connecting. Click enter and you are connected to Mindsdb API.

![](https://github.com/kinkusuma/mindsdb/blob/add-regression-tutorial-sql/docs/mindsdb-docs/docs/assets/sql/tutorials/insurance-cost-prediction/success-connect-sql.png)

If you have an authentication error, please make sure you are providing the email address you have used to create an account on MindsDB Cloud.

## Data

Now, let's show the databases.

![](https://github.com/kinkusuma/mindsdb/blob/add-regression-tutorial-sql/docs/mindsdb-docs/docs/assets/sql/tutorials/insurance-cost-prediction/show-databases-sql.png)

There are 4 databases, and the mysql database is the database that I've connected to Mindsdb.

Let's check mysql database.

![](https://github.com/kinkusuma/mindsdb/blob/add-regression-tutorial-sql/docs/mindsdb-docs/docs/assets/sql/tutorials/insurance-cost-prediction/show-tables-sql.png)

There are 3 tables, and because the tutorial is about insurance cost prediction, we will use the insurance table.  
And let's check, what is inside this table.

![](https://github.com/kinkusuma/mindsdb/blob/add-regression-tutorial-sql/docs/mindsdb-docs/docs/assets/sql/tutorials/insurance-cost-prediction/show-insurance-table.png)

So, these tables have 7 columns:

- age: The age of the person (integer)
- sex: Gender (male or female)
- bmi: Body mass index is a value derived from the mass and height of a person. 
The BMI is defined as the body mass divided by the square of the body height, and is expressed in units of kg/m², 
resulting from mass in kilograms and height in meters (float)
- children: The number of children (integer)
- smoker: Indicator if the person smoke (yes or no)
- region: Region where the insured lives (southeast, northeast, southwest or northwest)
- charges: The insurance cost, this is the target of prediction (float)

## Create the model

Now, to create the model let's move to mindsdb database. and let's see what's inside.

![](https://github.com/kinkusuma/mindsdb/blob/add-regression-tutorial-sql/docs/mindsdb-docs/docs/assets/sql/tutorials/insurance-cost-prediction/show-tables-sql-2.png)

There are 2 tables, predictors, and commands. Predictors contain your predictors record, and commands contain your last commands used.  
To train a new machine learning model we will need to CREATE Predictor as a new record inside the predictors table, and using this command:

```sql
CREATE PREDICTOR predictor_name
FROM integration_name
(SELECT column_name, column_name2 FROM table_name) as ds_name
PREDICT column_name as column_alias;
```

The values that we need to provide are:

* predictor_name (string) - The name of the model.
* integration_name (string) - The name of the connection to your database.
* ds_name (string) - the name of dataset you want to create, it's optional if you don't specify this value Mindsdb will generate by itself.
* column_name (string) - The feature you want to predict.
* column_alias - Alias name of the feature you want to predict.

So, use this command to create the models:

![](https://github.com/kinkusuma/mindsdb/blob/add-regression-tutorial-sql/docs/mindsdb-docs/docs/assets/sql/tutorials/insurance-cost-prediction/create-predictor-insurance-sql.png)

If there's no error, that means your model is created and training. To see if your model is finished, use this command:

```sql
SELECT * FROM mindsdb.predictors WHERE name = predictor_name;
```

And values that we need to provide are:

* predictor_name (string) - The name of the model.

![](https://github.com/kinkusuma/mindsdb/blob/add-regression-tutorial-sql/docs/mindsdb-docs/docs/assets/sql/tutorials/insurance-cost-prediction/show-predictor-isurance-sql.png)

If the model is finished, it will look like this. The model has been created! and the accuracy is 75%, you can still increasing the accuracy by cleaning the data, or doing some data wrangling techniques.

## Create the prediction

Now you are in the last step of this tutorial, creating the prediction. To create a prediction you can use this command:

```sql
SELECT target_variable, target_variable_explain FROM model_table 
                                                WHERE when_data='{"column3": "value", "column2": "value"}';
```

And you need to set these values:
- target_variable - The original value of the target variable.
- target_variable_confidence - Model confidence score.
- target_variable_explain - JSON object that contains additional information as confidence_lower_bound, confidence_upper_bound, anomaly, truth.
- when_data - The data to make the predictions from(WHERE clause params).

![](https://github.com/kinkusuma/mindsdb/blob/add-regression-tutorial-sql/docs/mindsdb-docs/docs/assets/sql/tutorials/insurance-cost-prediction/create-prediction-isurance-sql.png)

And now you have made an insurance predictor using SQL and Mindsdb. Yayyy!

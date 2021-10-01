# How to enable Automated Machine Learning in MySQL

A database is surely the best place for Machine Learning - because data is the main ingredient of it. And now you can build, train, test & query Machine Learning models using standard SQL queries within a MySQL database!

This doesn't require hardcore data science knowledge - the whole Machine Learning workflow is automated.

This solution is called AI-Tables and is available in MySQL thanks to integration with an open-source predictive engine from MindsDB.
AI-Tables look like normal database tables and return predictions upon being queried as if they were data that exists in the table. In plain SQL, it looks like this:

```sql
SELECT <predicted_variable> FROM <ML_model> WHERE <conditions>
```

This video explains how it works:

<iframe width="100%" height="415" src="https://www.youtube.com/embed/ymf_yYKahos" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture" allowfullscreen></iframe>

In this tutorial below, you will get step-by-step instructions on how to enable AI-Tables in a MySQL database. Based on a churn prediction example, you see how to build, train and query Machine Learning models by only using SQL statements with MindsDB!


## How to install MySQL?

If you don’t have MySQL installed, you can download the installers for various platforms from the [official documentation](https://www.mysql.com/downloads/).

## Example dataset

In this tutorial, we will use the [Churn Modelling Dataset
](https://www.kaggle.com/shrutimechlearn/churn-modelling). If you have other datasets in your MySQL database, please skip this section.

This data set contains details of a bank's customers, and the target variable is a binary variable reflecting whether the customer left the bank (closed their account) or is still a customer.

#### Import dataset to MySQL
The first thing we need to do is to import the dataset in MySQL. Create a new table called bank_churn:

```sql
-- test.bank_churn definition
CREATE TABLE test.bank_churn (
    CreditScore NUMERIC NULL,
    Geography varchar(100) NULL,
    Gender varchar(100) NULL,
    Age NUMERIC NULL,
    Tenure NUMERIC NULL,
    Balance NUMERIC NULL,
    NumOfProducts NUMERIC NULL,
    HasCrCard NUMERIC NULL,
    IsActiveMember NUMERIC NULL,
    EstimatedSalary NUMERIC NULL,
    Exited NUMERIC NULL
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_0900_ai_ci;
```

Next, we need to import the data inside the table. There are a few options to do that:


Using the Load Data statement:
```
LOAD DATA LOCAL INFILE 'data.csv' INTO bank_churn FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n';
```
Using MySQL import:

```
mysqlimport --local --fields-terminated-by="," bank_churn data.csv
```
Using pgAdmin, DBeaver or another SQL client -- just use the “import from CSV” file option from the navigation menu.

Let’s select some data from the bank_churn table to check that the data was successfully imported to MySQL:

```sql
SELECT * FROM bank_churn LIMIT 1;
```

![SELECT FROM bank_churn](/assets/tutorials/aitables-mysql/select_table.png)

## Add Configuration

As a prerequisite for using [MySQL](https://www.mysql.com/downloads/), we need to enable the Federated Storage engine. Check out the official [MySQL documentation](https://dev.mysql.com/doc/refman/8.0/en/federated-storage-engine.html) to see how to do that.

The last step is to create the MindsDB configuration file. MindsDB will try to use the default configuration options like host, port or username for MySQL. If you want to extend these or change the default values, you need to add a config.json file. Create a new file config.json and include the following information:

```json
{
  "api": {
      "http": {
          "host": "127.0.0.1",
          "port": "47334"
      },
      "mysql": {
          "host": "127.0.0.1",
          "password": "password",
          "port": "47335",
          "user": "root"
      }
  },
  "config_version": "1.4",
  "integrations": {
    "default_mysql": {
          "publish": true,
          "host": "localhost",
          "password": "root",
          "port": 3307,
          "type": "mysql",
          "user": "root"
    }
  },
  "storage_dir": "/storage"
}
```

The values provided in the configuration file are:

* api['http’] -- This key is used for starting the MindsDB HTTP server by providing:
host(default 127.0.0.1) - The mindsDB server address.
* port(default 47334) - The mindsDB server port.
* api['mysql'] -- This key is used for database integrations that work through MySQL protocol. The required keys are:
    * user(default root).
    * password(default empty).
    * host(default 127.0.0.1).
    * port(default 47335).
* integrations[default_mysql] -- This key specifies the integration type in this case default_mysql. The required keys are:
    * user(default root) - The MySQL user name.
    * host(default localhost) - Connect to the MySQL server on the given host.
    * password - The password of the MySQL account.
    * type - Integration type(mariadb, postgresql, mysql, clickhouse, mongodb).
    * port(default 5432) - The TCP/IP port number to use for the connection.
* storage_dir -- The directory where mindsDB will store models and configuration files.

Now, we have successfully set up all of the requirements for AI Tables in MySQL.

### AutoML with AI Tables in MySQL
If you don't have MindsDB installed, check out our [Installation guide](/Installing) and find an option that works for you. After that, start the MindsDB server:

```
python3 -m mindsdb --api=mysql --config=config.json
```

The arguments sent to MindsDB are:

* --api - This tells MindsDB which API should be started (HTTP or MySQL).
* --config - The path to the configuration file that we have created.
If everything works as expected, you should see the following message:

![MindsDB Started](/assets/tutorials/aitables-postgresql/mindsdb_started.png)


Upon successful setup, MindsDB should create a new database called mindsdb.

![MindsDB Schema](/assets/tutorials/aitables-mysql/list_tables.png)


In the mindsDB database, two new tables should be created, called commands and predictors. The mindsdb.predictors table is the table where MindsDB will keep information about trained models.

### Train new Machine Learning Model

Training the machine learning model using MindsDB is quite simple. It can be done by executing the `INSERT` query inside the mindsdb.predictors table. In our example, we want to predict if the bank's customer has left the bank from the bank_churn table, so let’s run an `INSERT` query as follows:

```sql
INSERT INTO mindsdb.predictors(name, predict, select_data_query)
VALUES ('churn_model', 'Exited', 'SELECT * FROM test.bank_churn');
```

This query will create a new model called 'churn_model' and a new table 'churn_model' inside mindsdb database. The required columns (parameters) added in the `INSERT` for training the predictor are:
* name (string) - the name of the predictor.
* predict (string) -  the feature you want to predict, in this example it will be Exited.
* select_data_query (string) - the SELECT query that will get the data from MySQL.

To check that the training successfully finished, we can `SELECT` from the mindsdb.predictors table and get the status:

```sql
SELECT * FROM mindsdb.predictors WHERE name='churn_model';
```

![Status](/assets/tutorials/aitables-mysql/select_status.png)

A status of “complete” means that training successfully finished. Now, let’s query the model.
The trained model behaves like an AI Table and can be queried as if it were a standard database table. To get the prediction, we need to execute a `SELECT` query and in the `WHERE` clause include the when_data as a JSON string that includes feature values, such as CreditScore, EstimatedSalary, Gender, Balance, etc.

```sql
SELECT *
FROM mindsdb.churn_model
WHERE when_data='{"CreditScore": "619","Geography": "France","Gender": "Female", "EstimatedSalary": 100000, "Balance": 0.0, "Age":42, "Tenure": 2}';
```

In a second we should get the prediction back from MindsDB. So, MindsDB thinks that the above customer closed their account with the bank (predicted_value 1) with around 98% confidence.

Information in JSON format in the explain column:

```json
{
"predicted_value": "1.0",
"confidence": 0.98,
"prediction_quality": "very confident",
"important_missing_information": ["NumOfProducts"]
}
```

The `important_missing_information` shows the list of features that MindsDB thinks are quite important for better prediction, in this case, the "NumOfProducts".
Congratulations, you have successfully trained and queried a Machine Learning Model by only using SQL Statements. Note that even if you used MySQL to build the model, you can still query the same model from the other databases too.

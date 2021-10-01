# Automated Machine Learning in Microsoft SQL Server

<iframe width="100%" height="415" src="https://www.youtube.com/embed/p9aficF_-Tk" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture" allowfullscreen></iframe>

Data is the main ingredient for machine learning. Nowadays most enterprise structured data lives inside a database, so moving the machine learning inside the database layer can bring great benefits. This is one of the main reasons we decided to build an integration with the most widely used database systems, so that any database user can create, train and query machine learning models with minimal knowledge of SQL.

![MindsDB integration](/assets/tutorials/aitables-mssql/AI Tables.jpg)


Using the MindsDB AI Tables feature, users can automate model training workflows and reduce deployment time and complexity by adding the predictive layer to the database.
AI Tables allow database users to query predictive models in seconds as if they were data that existed on the table. Simply explained, AI Tables allow users to use machine learning models as if they were normal database tables. In plain SQL, that looks like this:

```sql
SELECT <predicted_variable> FROM <ML_model> WHERE <conditions>
```

MindsDB acts like an AutoML platform and interacts with your SQL database through an ODBC driver. AI Tables accelerate development speed, reduce costs and eliminate the complexity of machine learning workflows.

* Train models from the data inside the SQL database
* Get predictions through simple SQL queries
* Get insights into model accuracy
* Move from idea to production in minutes

In this tutorial, you will get step-by-step instructions on how to enable AI Tables in Microsoft SQL Server. Based on a medical insurance example dataset, you will see how to train and query machine learning models by using only SQL statements.


## How to install MindsDB

If you already have MindsDB installed, you can skip this section. MindsDB is a cross-platform tool and can be installed on the most widely used OSs: Windows, Linux and macOS. Also, there are few other options to get MindsDB, such as through Docker, installing from source code or using PyPi. To get started, check the following list, and use the option that works best for you:

* [Windows](/installation/windows)
* [Linux](/installation/linux)
* [MacOS](/installation/macos)
* [Docker](/installation/docker)
* [Source install](/installation/source)

## How to install Microsoft SQL Server

Head over to the official [SQL Server installation guide](https://docs.microsoft.com/en-us/sql/database-engine/install-windows/install-sql-server?view=sql-server-ver15) on Microsoft docs for Windows. If you want to use the cloud version, check out [this documentation](https://www.microsoft.com/en-us/sql-server/sql-server-downloads).

### Prerequisite

For this integration to work, you must make sure that the Microsoft SQL Server has [MSDASQL](https://docs.microsoft.com/en-us/sql/ado/guide/appendixes/microsoft-ole-db-provider-for-odbc?view=sql-server-ver15) installed, which currently only  works on Windows machines. If not, you can download it [here](https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver15).


## Example dataset

In this tutorial, we will use the [Medical Cost Personal](https://www.kaggle.com/mirichoi0218/insurance) dataset from Kaggle. The data contains medical information and costs billed by health insurance companies. There are 1338 observations and 7 variables in this dataset:

* age: age of the primary beneficiary
* sex: insurance contractor gender – female, male
* bmi: body mass index - an objective index of body weight (kg / m2) using the ratio of height to weight, ideally 18.5 to 24.9
* children: number of children covered by health insurance / number of dependents
* smoker: smoker?
* region: the beneficiary's residential area in the US – northeast, southeast, southwest, northwest
* charges: individual medical costs billed by the health insurance.

The end result that we will try to achieve is to accurately predict insurance costs per beneficiary.

## Import dataset to Microsoft SQL Server

If you are following this tutorial with your own data, you can skip this section. The first thing we need to do before importing the dataset is to create a new table called `insurance`:

```sql
CREATE TABLE insurance (
    age int NULL,
    sex varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    bmi numeric(18,0) NULL,
    children int NULL,
    smoker varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    region varchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
    charges numeric(18,0) NULL
) GO;
```

To import the dataset there are a few options available. If you are using [SQL Management Studio](https://support.discountasp.net/kb/a1179/how-to-import-a-csv-file-into-a-database-using-sql-server-management-studio.aspx) or any other database management GUI, use the import from CSV option. If you are following this tutorial using a SQL client, you can use the `BULK INSERT` statement:

```sql
BULK INSERT insurance
FROM 'insurance.csv'
WITH
(
   FORMAT = 'CSV',
   FIELDQUOTE = '"',
   FIRSTROW = 2,
   FIELDTERMINATOR = ',',  -- the insurance.csv delimiter
   ROWTERMINATOR = '\n', 
   TABLOCK
)
```

Now, the data should be inside the `insurance` table. To make sure it was successfully imported, `SELECT` one row from the created table:

```sql
SELECT TOP(1) * FROM insurance;
```

![SELECT FROM insurance](/assets/tutorials/aitables-mssql/select-db.png)

## Connect MindsDB and Microsoft SQL Server

We have installed MindsDB and imported the data, so the next step is to create the integration.
When you start MindsDB, it should automatically run the MindsDB Studio (Graphical User Interface) at the  `http://127.0.0.1:47334/` address in your browser. We can use the Studio to connect MindsDB and Microsoft SQL server:

1. From the left navigation menu, select Database Integration.
2. Click on the `ADD DATABASE` button.
3. In the `Connect to Database` modal window:
   1. Select `Microsoft SQL Server` as the Supported Database.
   2. Add the Database name.
   3. Add the Host name.
   4. Add Port.
   5. Add SQL Server user.
   6. Add Password for SQL Server user.
   7. Click on `check connection` to test the connection.
   7. Click on `CONNECT`.


![Connect to MSSQL](/assets/data/mssql.gif)

The next step is to create a new datasource by selecting the data from the `insurance` table. Click on the `NEW DATASOURCE` button, and add the `SELECT` query in the `Query` input:

```
SELECT * FROM insurance;
```

![Create MSSQL Datasource](/assets/data/mssql-ds.gif)

We have created a new datasource `InsuranceData`. What we did through the MindsDB Studio can also be done by extending the default configuration of the MindsDB Server. To learn more about how to do that, see the [mssql-client](/datasources/mssql/#mssql-client) docs.

## Train new model

The connection between the MindsDB and Microsoft SQL Server is done, so the next step is to train the model. Training a new model is done by inserting into the `mindsdb.predictors` table. The required parameters that the `INSERT` query should have are:

* name (string) -- The name of the machine learning model that we will train.
* predict (string) -- The column (feature) that we want to get predictions for.
* select_data_query (string) -- The `SELECT` query that will select the data used for training.

In this example we will train new model called `insurance_model` that predicts the `charges` from the `insurance` table, so the model training query looks like this:

```sql
exec ('INSERT INTO mindsdb.predictors (name, predict, select_data_query)
VALUES ("insurance_model", "charges", "SELECT * FROM insurance")')
AT mindsdb;
```

![Train model from mssql client](/assets/tutorials/aitables-mssql/train-model.png)

If you are wondering why we are using the `exec` command to wrap the `INSERT` statement, that's because it's one of the ways to query a linked server from Microsoft SQL Server. An alternate option is to use [openquery](https://docs.microsoft.com/en-us/sql/t-sql/functions/openquery-transact-sql?view=sql-server-ver15), which will execute the specified `INSERT` query on the server. In this example the query looks like this:

```sql
INSERT openquery(mindsdb,'SELECT name, predict, select_data_query FROM mindsdb.predictors WHERE 1=0') VALUES ('insurance_model','charges','SELECT * FROM insurance');
```

Now, in the background, the training has started. MindsDB will do a black-box analysis and start a process of extracting, analyzing, and transforming the data. The time it takes to train a model differs depending on dataset size, the number of features, feature types, etc. To check if the model training has successfully finished, we can `SELECT` from the `mindsdb.predictors` table as:


```sql
exec ('SELECT * FROM mindsdb.predictors') AT mindsdb;
```

![Training model status](/assets/tutorials/aitables-mssql/model-status.png)

The query will return the status of the model as `complete`, or if the training is still running as `training`.


## Make predictions (Query the model)

Once the model training finishes we can query that model. Querying the model can be done by executing the `SELECT` statement from the models `AI Table`. Let's try and query the `insurance_model` that we have created.
The prediction that we will do is to get insurance for the beneficiary that is 30 years old, has 27.9 as a body mass index and has 1 child. All of these parameters should be added inside the `WHERE` clause.

```sql
exec('SELECT charges AS predicted,
            charges_confidence AS confidence,
            charges_explain AS info
     FROM mindsdb.insurance_model
     WHERE age=30 AND bmi=27.9 AND children=1') AT mindsdb;
```

![Query model](/assets/tutorials/aitables-mssql/query.png)

To get the insurance cost, confidence in that cost, and additional information from MindsDB, we did a `SELECT` for `charges`, `charges_confidence`, and `charges_explain`. The response from MindsDB should look like this:

```json
predicted  | 9421.233985124825
confidence | 0.86
info       | {"predicted_value": 9421.233985124825,
             "confidence": 0.86,
             "prediction_quality": "very confident",
             "confidence_interval": [6603.107461750912, 12239.36050849874],
             "important_missing_information": ["smoker"]
           }
```

MindsDB thinks that for this beneficiary, the insurance cost should be 9,421, with 86% confidence for this prediction. The `charges` cost interval could vary from 6,603 to 12,239, depending on the beneficiary. As the important information for improving this prediction, MindsDB thinks that providing  information about `smoker` is very important.

## Conclusion

We have seen how easy it is to train and query models directly from a Microsoft SQL database. In short, the flow that we covered was:

* Connect to Microsoft SQL Server using MindsDB Studio integrations dashboard.
* Create a new datasource by `SELECT`ing from the table.
* Train new model by `INSERT`ing in mindsdb.predictors table.
* Get predictions by `SELECT`ing from the model.

If you want to try AI Tables in a different database, check out the other tutorials:

* [AI Tables in MySQL](/tutorials/mysql)
* [AI Tables in MariaDB](/tutorials/mariadb)
* [AI Tables in PostgreSQL](/tutorials/postgresql)
* [AI Tables in ClickHouse](/tutorials/clickhouse)


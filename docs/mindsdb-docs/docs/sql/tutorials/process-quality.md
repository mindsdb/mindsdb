# Manufacturing process quality

Predicting process result quality is a common task in manufacturing analytics. Manufacturing plants commonly use quality predictions to gain a competitive edge over their competitors, improve their products or increase their customers satisfaction. **MindsDB** is a tool that can help you solve quality prediction tasks **easily** and **effectively**

In this tutorial you will learn how to predict the quality of a mining process using **MindsDB** and **PostgreSQL**.

## Pre-requisites

Before you start make sure you have:

1. Access to MindsDB. Check out the installation guide for [Docker](https://docs.mindsdb.com/deployment/docker/) or [PyPi](https://docs.mindsdb.com/deployment/pypi/). You can also use [MindsDB Cloud](https://docs.mindsdb.com/deployment/cloud/).
2. Access to a PostgreSQL database. You can [install locally](https://www.postgresql.org/download/) or use a managed service like [Heroku Postgres](https://www.heroku.com/postgres).
3. Downloaded the dataset. You can get it from [Kaggle](https://www.kaggle.com/edumagalhaes/quality-prediction-in-a-mining-process).
4. Access to mysql-client. You can probably get it from your systems package manager. For Debian/Ubuntu check [here](https://packages.ubuntu.com/search?keywords=mysql-client).
4. Optional: Access to ngrok. You can check the installation details at the [ngrok website](https://ngrok.com/).

## Setup the database

In this section you will create a PostgreSQL database and a table into which you will then load the dataset.

First connect to your PostgreSQL instance. You can use the CLI based `psql` or any manager like [PgAdmin](https://www.pgadmin.org/).

If you have PostgreSQL running locally, you can use the following line to connect.

> Remember to change the username if you have a different one set up in PostgreSQL.

```bash
psql -h localhost -U postgres
```

After connecting you can create a database for the project. You can skip this step if you already have the database you want to use.

```sql
CREATE DATABASE manufacturing;
```

Next, you need to create a table for the dataset. To do so, first switch to the database you want to use.

```sql
\c manufacturing
```

Now you can create the table with the following schema.

```sql
CREATE TABLE process_quality (
    "date" date,
    "% Iron Feed" decimal,
    "% Silica Feed" decimal,
    "Starch Flow" decimal,
    "Amina Flow" decimal,
    "Ore Pulp Flow" decimal,
    "Ore Pulp pH" decimal,
    "Ore Pulp Density" decimal,
    "Flotation Column 01 Air Flow" decimal,
    "Flotation Column 02 Air Flow" decimal,
    "Flotation Column 03 Air Flow" decimal,
    "Flotation Column 04 Air Flow" decimal,
    "Flotation Column 05 Air Flow" decimal,
    "Flotation Column 06 Air Flow" decimal,
    "Flotation Column 07 Air Flow" decimal,
    "Flotation Column 01 Level" decimal,
    "Flotation Column 02 Level" decimal,
    "Flotation Column 03 Level" decimal,
    "Flotation Column 04 Level" decimal,
    "Flotation Column 05 Level" decimal,
    "Flotation Column 06 Level" decimal,
    "Flotation Column 07 Level" decimal,
    "% Iron Concentrate" decimal,
    "% Silica Concentrate" decimal
);
```

You can check if the table was created with the `\dt` command. You should see a similar output.

```
              List of relations
 Schema |      Name       | Type  |  Owner
--------+-----------------+-------+----------
 public | process_quality | table | postgres
(1 row)
```



When the table is created you can load the dataset into it.

Unfortunately the data comes with commas as a decimal separator. This is not supported with PostgreSQL. A simple way of handling that is to convert all commas in the CSV file to dots and then changing the delimiter to a dot when loading the dataset. You can do this with `sed`.

```bash
sed -i 's/,/./g' MiningProcess_Flotation_Plant_Database.csv
```

To load the CSV file into the table use the following query.
> Remember to change the path to the dataset to match the file location on your system.

```sql
COPY process_quality
FROM '/path/to/dataset/MiningProcess_Flotation_Plant_Database.csv'
DELIMITER '.'
CSV HEADER;
```

To verify that the data has been loaded, you can make a simple `SELECT` query.

```sql
SELECT "% Iron Feed", "% Silica Feed", "Starch Flow" FROM process_quality LIMIT 10;
```

You should see a similar output.

```
 % Iron Feed | % Silica Feed | Starch Flow
-------------+---------------+-------------
        55.2 |         16.98 |     3019.53
        55.2 |         16.98 |     3024.41
        55.2 |         16.98 |     3043.46
        55.2 |         16.98 |     3047.36
        55.2 |         16.98 |     3033.69
        55.2 |         16.98 |      3079.1
        55.2 |         16.98 |     3127.79
        55.2 |         16.98 |     3152.93
        55.2 |         16.98 |     3147.27
        55.2 |         16.98 |     3142.58
(10 rows)
```

The last step is to change the table column names so that they don't contain spaces and special characters. This will simplify things later. Use the following queries to change the column names.

```sql
ALTER TABLE process_quality
RENAME COLUMN "% Iron Feed" TO iron_feed;
ALTER TABLE process_quality
RENAME COLUMN "% Silica Feed" TO silica_feed;
ALTER TABLE process_quality
RENAME COLUMN "Starch Flow" TO starch_flow;
ALTER TABLE process_quality
RENAME COLUMN "Amina Flow" TO amina_flow;
ALTER TABLE process_quality
RENAME COLUMN "Ore Pulp Flow" TO ore_pulp_flow;
ALTER TABLE process_quality
RENAME COLUMN "Ore Pulp pH" TO ore_pulp_ph;
ALTER TABLE process_quality
RENAME COLUMN "Ore Pulp Density" TO ore_pulp_density;
ALTER TABLE process_quality
RENAME COLUMN "Flotation Column 01 Air Flow" TO floatation_column_01_airflow;
ALTER TABLE process_quality
RENAME COLUMN "Flotation Column 02 Air Flow" TO floatation_column_02_airflow;
ALTER TABLE process_quality
RENAME COLUMN "Flotation Column 03 Air Flow" TO floatation_column_03_airflow;
ALTER TABLE process_quality
RENAME COLUMN "Flotation Column 04 Air Flow" TO floatation_column_04_airflow;
ALTER TABLE process_quality
RENAME COLUMN "Flotation Column 05 Air Flow" TO floatation_column_05_airflow;
ALTER TABLE process_quality
RENAME COLUMN "Flotation Column 06 Air Flow" TO floatation_column_06_airflow;
ALTER TABLE process_quality
RENAME COLUMN "Flotation Column 07 Air Flow" TO floatation_column_07_airflow;
ALTER TABLE process_quality
RENAME COLUMN "Flotation Column 01 Level" TO floatation_column_01_level;
ALTER TABLE process_quality
RENAME COLUMN "Flotation Column 02 Level" TO floatation_column_02_level;
ALTER TABLE process_quality
RENAME COLUMN "Flotation Column 03 Level" TO floatation_column_03_level;
ALTER TABLE process_quality
RENAME COLUMN "Flotation Column 04 Level" TO floatation_column_04_level;
ALTER TABLE process_quality
RENAME COLUMN "Flotation Column 05 Level" TO floatation_column_05_level;
ALTER TABLE process_quality
RENAME COLUMN "Flotation Column 06 Level" TO floatation_column_06_level;
ALTER TABLE process_quality
RENAME COLUMN "Flotation Column 07 Level" TO floatation_column_07_level;
ALTER TABLE process_quality
RENAME COLUMN "% Iron Concentrate" TO iron_concentrate;
ALTER TABLE process_quality
RENAME COLUMN "% Silica Concentrate" TO silica_concentrate;
```

You have now finished setting up the PostgreSQL database! 🐘

## Connect MindsDB to your database

In this section, you will connect your database to MindsDB.

The recommended way of connecting a database to MindsDB is through MindsDB GUI. In this tutorial we will use the GUI at MindsDB Cloud.

Since our PostgreSQL instance is local we will use `ngrok` to make it available to MindsDB Cloud. If you are using a PostgreSQL instance that already has a public address or you have installed MindsDB locally you can skip this step.

First you need to setup an ngrok tunnel with the following command.
> If you have used a different port for your PostgreSQL installation, remember to change it here.

```bash
ngrok tcp 5423
```

You should see a similar output:

```console
Session Status                online
Account                       myaccount (Plan: Free)
Version                       2.3.40
Region                        United States (us)
Web Interface                 http://127.0.0.1:4040
Forwarding                    tcp://x.tcp.ngrok.io:12345 -> localhost:5432
```
Now you can copy the forwarded address from the above output. You are interested in the `x.tcp.ngrok.io:12345` part.

With the address copied, head over to MindsDB GUI.

In the main screen, select `ADD DATABASE`. Then add your integration details.

![MindsDB PostgreSQL integration details](/docs/mindsdb-docs/docs/assets/sql/tutorials/process-quality/database-integration.png)

Click `Connect`, you should now see your PostgresSQL database connection in the main screen.

You are now done with connecting MindsDB to your database! 🚀

## Create a predictor

In this section you will connect to MindsDB with the MySql API and create a predictor.

Use the following query to create a predictor that will predict the silica_concentrate at the end of our mining process.
> The row number is limited to 5000 to speed up training but you can keep the whole dataset.
```sql
CREATE PREDICTOR process_quality_predictor
FROM process_quality_integration (
    SELECT * FROM process_quality LIMIT 5000
) PREDICT silica_concentrate as quality USING {"ignore_columns": ["date"]};
```

After creating the predictor you should se a similar output:

```console
Query OK, 0 rows affected (2 min 27.52 sec)
```

Now the predictor will begin training. You can check the status of the predictor with the following query.

```sql
SELECT * FROM mindsdb.predictors WHERE name='quality_predictor';
```

After the predictor has finished training, you will see a similar output.

```console
+-----------------------------+----------+----------+--------------------+-------------------+---------------------+------------------+
| name                        | status   | accuracy | predict            | select_data_query | external_datasource | training_options |
+-----------------------------+----------+----------+--------------------+-------------------+---------------------+------------------+
| process_quality_predictor   | complete | 1        | silica_concentrate |                   |                     |                  |
+-----------------------------+----------+----------+--------------------+-------------------+---------------------+------------------+
1 row in set (0.28 sec)
```

As you can see the accuracy of the model is 1. This is the result of using a limited dataset of 5000 rows. In reality when using the whole dataset, you will probably see a more reasonable accuracy.

You are now done with creating the predictor! ✨

## Make predictions

In this section you will learn how to make predictions using your trained model.

To run a prediction against new or existing data, you can use the following query.

```sql
SELECT silica_concentrate, silica_concentrate_confidence, silica_concentrate_explain as Info
FROM mindsdb.process_quality_predictor_1
WHERE when_data='{"iron_feed": 48.81, "silica_feed": 25.31, "starch_flow": 2504.94, "amina_flow": 309.448, "ore_pulp_flow": 377.6511682692, "ore_pulp_ph": 10.0607, "ore_pulp_density": 1.68676}';
```

The output should look similar to this.
```console
+--------------------+-------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------+
| silica_concentrate | silica_concentrate_confidence | Info                                                                                                                                            |
+--------------------+-------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------+
| 1.68               | 0.99                          | {"predicted_value": "1.68", "confidence": 0.99, "confidence_lower_bound": null, "confidence_upper_bound": null, "anomaly": null, "truth": null} |
+--------------------+-------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------+
1 row in set (0.81 sec)
```

As you can see, the model predicted the `silica concentrate` for our data point. Again we can see a very high confidence due to the limited dataset. When making predictions you can include different fields. As you can notice, we have only included the first 7 fields of our dataset. You are free to test different combinations.

You are now done with the tutorial! 🎉

For more check out other [tutorials and MindsDB documentation](https://docs.mindsdb.com/).
# Crop Recomendation

*Dataset: [Crop recomendation Data](https://www.kaggle.com/atharvaingle/crop-recommendation-dataset)*

*Communtiy Author: [pixpack](https://github.com/pixpack)*

Modern agriculture is becoming very dependent on technology. From advanced machinery to specially selected crops. All the technology produces a lot of data that can be used for better adjustment of the farming process. One use case of machine learning in agriculture could be the selection of the best crop for a specific field to maximize the potential yield. Such problems are often called *Classification Problems* in machine learning. With **MindsDB** you can easily make automated machine learning predictions straight from your existing database. Even without advanced ML engineering skills, you can start leveraging predictive models that help you make better business decisions.

In this tutorial, you will learn how to predict the best crop type based on field parameters using **MindsDB** and **MariaDB**.

## Pre-requisites

Before you start make sure you have:

1. Access to MindsDB. In this tutorial, we will use [MindsDB Cloud](https://docs.mindsdb.com/deployment/cloud/). If you want you can also deploy mindsdb on your premises, Check out the installation guide for [Docker](https://docs.mindsdb.com/deployment/docker/) or [PyPi](https://docs.mindsdb.com/deployment/pypi/). 

2. Downloaded the dataset. You can get it from [Kaggle](https://www.kaggle.com/atharvaingle/crop-recommendation-dataset).

3. Access to a mysql-client. [Docker](https://docs.mindsdb.com/faq/mysql-client/)



## Add your file to MindsDB

MindsDB can integrates with many databases, in most scenarios your data will be stored in a database, if you decide to load this dataset into your database of choice, please follow instructions here as to how to [connect mindsdb to your database](https://docs.mindsdb.com/faq/plug_your_data).

In this tutorial, you simply upload the kaggle file Crop_recommendation.csv to MindsDB via the MindsDB adming GUI, In this tutorial, we are using [cloud.mindsdb.com](https://cloud.mindsdb.com). Alternatively, remember that if you are using a local deployment you will have to point your browser to [127.0.0.1:47334](https://127.0.0.1:47334).


In the main screen, select `FILES` > `FILE UPLOAD`. Then add your file and give it a name, in this tutorial we will name it `crops`.


Click `Upload`, you should now see your file on the list of files. 🚀


NOTE: Mindsdb SQL Server allows you to query your uploaded files using SQL, we will get to that right now.

## Connecting to your MindsDB SQL Server

In this section you will connect to MindsDB with the MySQL API and explore what is there for you.

First you need to connect to MindsDB through the MySQL API. To do so, use the following command.

> Remember to change the MindsDB Cloud username for the connection

```bash
mysql -h cloud.mindsdb.com --port 3306 -u cloudusername@mail.com -p
```


Note: If you are using a local deployment, please review [Connect to your Local deployment](https://docs.mindsdb.com/faq/local_deployment/)


After that you can list the provided databases.

```sql
SHOW databases;
```

You will notice that there is a database called `files`.

```sql
USE files;
```

You can explore your data

```sql
SELECT * from crops limit 4;
```



## Create a predictor

Now we can create a machine learning model with `crops` columns serving as features, and MindsDB takes care of the rest of ML workflow automatically. There is a way to get your hands into the insides of the model to fine tune it, but we will not cover it in this tutorial.


Switch to the `mindsdb` database.

```sql
USE mindsdb;
```

Use the following query to create a predictor that will predict the `label` (*crop type*) for the specific field parameters.

```sql
CREATE PREDICTOR crop_predictor
FROM files (
    SELECT * FROM crops
) PREDICT label as crop_type;
```

After creating the predictor you should see a similar output:

```console
Query OK, 0 rows affected (11.66 sec)
```

Now the predictor will begin training. You can check the status of the predictor with the following query.

```sql
SELECT * FROM mindsdb.predictors WHERE name='crop_predictor';
```

After the predictor has finished training, you will see a similar output. Note that MindsDB does model testing for you automatically, so you will immediately see if the predictor is accurate enough.

```console
+-----------------+----------+--------------------+---------+---------------+-----------------+-------+-------------------+------------------+
| name            | status   | accuracy           | predict | update_status | mindsdb_version | error | select_data_query | training_options |
+-----------------+----------+--------------------+---------+---------------+-----------------+-------+-------------------+------------------+
|  crop_predictor | complete | 0.9954545454545454 | label   | up_to_date    | 2.55.2          |       |                   |                  |
+-----------------+----------+--------------------+---------+---------------+-----------------+-------+-------------------+------------------+
1 row in set (0.29 sec)

```

You are now done with creating the predictor! ✨

## Make predictions

In this section you will learn how to make predictions using your trained model.

To run a prediction against new or existing data, you can use the following query.

```sql
SELECT label
FROM mindsdb.crop_predictor
WHERE N = 77 and P = 52 and K = 17 and temperature = 24 and humidity = 20.74 and ph = 5.71 and  rainfall = 75.82
```

```console
label: maize
1 row in set (0.32 sec)
```

As we have used a real data point from our dataset we can verify the prediction.
```text
N,  P,  K,  temperature,  humidity,   ph,           rainfall,     label
77, 52, 17, 24.86374934,  65.7420046, 5.714799723,  75.82270467,  maize
```
 
As you can see, the model correctly predicted the most appropriate crop type for our field.

OK, we made a prediction using a single query, but what if you want to make a batch prediction for a large set of data in your database? In this case, MindsDB allows you to Join this other table with the Predictor. In result, you will get another table as an output with a predicted value as one of its columns.

Let’s see how it works.

Use the following command to create the batch prediction.

```sql
SELECT 
    collected_data.N,
    collected_data.P,
    collected_data.K,
    collected_data.temperature,
    collected_data.humidity,
    collected_data.ph,
    collected_data.rainfall,
    predictions.label as predicted_crop_type
FROM crops_integration.crops AS collected_data
JOIN mindsdb.crop_predictor AS predictions
LIMIT 5;
```

As you can see below, the predictor has made multiple predictions for each data point in the `collected_data` table! You can also try selecting other fields to get more insight on the predictions. See the [JOIN clause documentation](https://docs.mindsdb.com/sql/api/join/) for more information.

```console
+------+------+------+-------------+----------+------+----------+---------------------+
| N    | P    | K    | temperature | humidity | ph   | rainfall | predicted_crop_type |
+------+------+------+-------------+----------+------+----------+---------------------+
| 90   | 42   | 43   | 21          | 82.0     | 6.5  | 202.94   | rice                |
| 85   | 58   | 41   | 22          | 80.32    | 7.04 | 226.66   | rice                |
| 60   | 55   | 44   | 23          | 82.32    | 7.84 | 263.96   | rice                |
| 74   | 35   | 40   | 26          | 80.16    | 6.98 | 242.86   | rice                |
| 78   | 42   | 42   | 20          | 81.6     | 7.63 | 262.72   | rice                |
+------+------+------+-------------+----------+------+----------+---------------------+
```
You are now done with the tutorial! 🎉

Please feel free to try it yourself. Sign up for a [free MindsDB account](https://cloud.mindsdb.com/signup?utm_medium=community&utm_source=ext.%20blogs&utm_campaign=blog-crop-detection) to get up and running in 5 minutes, and if you need any help, feel free to ask in [Slack](https://join.slack.com/t/mindsdbcommunity/shared_invite/zt-o8mrmx3l-5ai~5H66s6wlxFfBMVI6wQ) or [Github](https://github.com/mindsdb/mindsdb/discussions).

For more check out other [tutorials and MindsDB documentation](https://docs.mindsdb.com/).
Welcome to the Quickstart Guide for using MindsDB. This README is designed to provide developers with an efficient and straightforward introduction to integrating and utilizing MindsDB in your projects. To start, follow the steps bellow:


## 1. Create a MindsDB Cloud Account or Install MindsDB Locally

Create your [free MindsDB Cloud account](https://cloud.mindsdb.com/register) to
start practicing right away using the MindsDB Cloud Editor.

If you prefer a local MindsDB installation, follow the **Deployment** guides of
MindsDB documentation. You can install MindsDB using
[Docker](setup/self-hosted/docker/) or follow the standard installation using
[pip](setup/self-hosted/pip/source/).

## 2. Connect to MindsDB from a SQL Client

You can use the MindsDB Editor or open your preferred SQL client, such
as DBeaver or MySQL CLI, and connect to MindsDB.

Learn more [here](/mindsdb-sql-overview).

## 3. Connect a Database Using [`CREATE DATABASE`](/sql/create/databases/)

We have a sample database that you can use right away. To connect a database to your MindsDB Cloud account, use the [`CREATE DATABASE`](/sql/create/databases/) statement, as below.

```sql
CREATE DATABASE example_data
WITH ENGINE = "postgres",
PARAMETERS = {
  "user": "demo_user",
  "password": "demo_password",
  "host": "3.220.66.106",
  "port": "5432",
  "database": "demo"
};
```

On execution, we get:

```sql
Query OK, 0 rows affected (3.22 sec)
```

## 4. Preview the Available Data Using [`SELECT`](/sql/api/select/)

You can now preview the available data with a standard `SELECT` statement.

```sql
SELECT *
FROM example_data.demo_data.home_rentals
LIMIT 10;
```

On execution, we get:

```sql
+-----------------+---------------------+------+----------+----------------+---------------+--------------+--------------+
| number_of_rooms | number_of_bathrooms | sqft | location | days_on_market | initial_price | neighborhood | rental_price |
+-----------------+---------------------+------+----------+----------------+---------------+--------------+--------------+
| 0.0             | 1.0                 | 484  | great    | 10             | 2271          | south_side   | 2271         |
| 1.0             | 1.0                 | 674  | good     | 1              | 2167          | downtown     | 2167         |
| 1.0             | 1.0                 | 554  | poor     | 19             | 1883          | westbrae     | 1883         |
| 0.0             | 1.0                 | 529  | great    | 3              | 2431          | south_side   | 2431         |
| 3.0             | 2.0                 | 1219 | great    | 3              | 5510          | south_side   | 5510         |
| 1.0             | 1.0                 | 398  | great    | 11             | 2272          | south_side   | 2272         |
| 3.0             | 2.0                 | 1190 | poor     | 58             | 4463          | westbrae     | 4124         |
| 1.0             | 1.0                 | 730  | good     | 0              | 2224          | downtown     | 2224         |
| 0.0             | 1.0                 | 298  | great    | 9              | 2104          | south_side   | 2104         |
| 2.0             | 1.0                 | 878  | great    | 8              | 3861          | south_side   | 3861         |
+-----------------+---------------------+------+----------+----------------+---------------+--------------+--------------+
```

You could also browse the databases of MindsDB using the command below.

```sql
SHOW databases;
```

On execution, we get:

```sql
+---------------------+
| Database            |
+---------------------+
| information_schema  |
| mindsdb             |
| files               |
| example_data        |
+---------------------+
```

To learn more about MindsDB tables structure, check out
[this guide](/sql/table-structure/).

## 5. Create a Model Using [`CREATE MODEL`](/sql/create/model/)

Now you are ready to create your first model. Use the
[`CREATE MODEL`](/sql/create/model/) statement, as below.

```sql
CREATE MODEL mindsdb.home_rentals_model
FROM example_data
  (SELECT * FROM demo_data.home_rentals)
PREDICT rental_price;
```

On execution, we get:

```sql
Query OK, 0 rows affected (9.79 sec)
```

## 6. Check the Status of a Model

It may take a couple of minutes until the model is trained. You can monitor
the status of your model by executing the following command:

```sql
DESCRIBE home_rentals_model;
```

On execution, we get:

```sql
+------------+
| status     |
+------------+
| generating |
+------------+
```

After a short time, we get:

```sql
+----------+
| status   |
+----------+
| training |
+----------+
```

And finally, we get:

```sql
+----------+
| status   |
+----------+
| complete |
+----------+
```

Alternatively, you can use the `SHOW MODELS` command as below.

```sql
SHOW MODELS
[FROM project_name]
[LIKE 'model_name']
[WHERE column_name = value];
```

Here is an example:

```sql
SHOW MODELS
FROM mindsdb
LIKE 'home_rentals_model'
WHERE status = 'complete';
```
> The status of the model must be `complete` before you can start making predictions.

## 7. Make Predictions Using [`SELECT`](/sql/api/select/)

The [`SELECT`](/sql/api/select/) statement allows you to make predictions based
on features, where features are the input variables, or input columns, that are
used to make forecasts.

Let's predict what would be the rental price of a 1000 square feet house with
two bathrooms.

```sql
SELECT rental_price
FROM mindsdb.home_rentals_model
WHERE number_of_bathrooms = 2
AND sqft = 1000;
```

On execution, we get:

```sql
+--------------+
| rental_price |
+--------------+
| 1130         |
+--------------+
```

Here is how to make batch predictions:

```sql
SELECT m.rental_price, m.rental_price_explain
FROM mindsdb.home_rentals_model AS m
JOIN example_data.demo_data.home_rentals AS d;
```

## 8. Automate the Workflow Using [`CREATE JOB`](/sql/create/jobs/)

Now, we can take this even further. MindsDB includes powerful automation features called Jobs which allow us to automate queries in MindsDB. This is very handy for production AI/ML systems which all require automation logic to help them to work.

We use the `CREATE JOB` statement to create a Job.

Now, let's use a Job to set the model we've created to be retrained every two days, just like we might in production. You can [retrain](/sql/api/retrain/) the model to improve predictions every time when either new data or new MindsDB version is available. And, if you want to retrain your model considering only new data, then go for [finetuning](/sql/api/finetune/) it.

In the same job, we will create a table and insert these new predictions back into a database so the predictions are ready to be used by our hypothetical application.

```sql
CREATE JOB retrain_model_and_save_predictions (

   RETRAIN mindsdb.home_rentals_model
   FROM example_data
         (SELECT * FROM demo_data.home_rentals)
   USING
         join_learn_process = true;

   CREATE TABLE my_integration.rentals_{{START_DATETIME}} (
         SELECT m.rental_price, m.rental_price_explain
         FROM mindsdb.home_rentals_model AS m
         JOIN example_data.demo_data.home_rentals AS d
   )

)
END '2023-10-30 00:00:00'
EVERY 2 days;
```

Please note that `my_integration` is your database connection name in MindsDB. Before executing this job, make sure to connect your database to MindsDB with a user who has write access to be able to create a table.


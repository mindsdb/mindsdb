# Quickstart Guide

Follow these steps to start predicting in SQL straight away.

<sup><sub>To try MindsDB with your own data or model, check out our [Getting Started Guide](/getting-started/).</sub></sup>

1. Create your [free MindsDB Cloud account](https://cloud.mindsdb.com/signup).

    <sup><sub>To proceed with a local installation, follow our [Docker instructions](/deployment/docker).</sub></sup>

1. Open your SQL client and connect to MindsDB using the email and password you used to sign up for MindsDB Cloud.

    <sup><sub>If you do not already have a preferred SQL client, we recommend [DBeaver Community Edition](https://dbeaver.io/download/)</sub></sup>

1. We have already prepared some example data for you.  To add it to your account, use the `CREATE DATASOURCE` syntax by copy and pasting this command into your SQL client:

    ``` sql
    CREATE DATASOURCE example_data
    WITH ENGINE = "postgres",
    PARAMETERS = { 
    	"user": "demo_user",
    	"password": "demo_password",
    	"host": "3.220.66.106",
    	"port": "5432",
    	"database": "demo"
    }
    ```
    <div id="create-datasource">
      <style>
        #create-datasource code { background-color: #353535; color: #f5f5f5 }
      </style>
    ```
    mysql> CREATE DATASOURCE example_data
        -> WITH ENGINE = "postgres",
        -> PARAMETERS = {
        -> "user": "demo_user",
        -> "password": "demo_password",
        -> "host": "3.220.66.106",
        -> "port": "5432",
        -> "database": "demo"
        -> }
    Query OK, 0 rows affected (3.22 sec)
    ```
    </div>

1. You can now preview the available data with a standard `SELECT`.  To preview the Home Rentals dataset, copy and paste this command into your SQL client:

    ```
    SELECT * 
    FROM example_data.demo_data.home_rentals
    LIMIT 10;
    ```
    <div id="preview-data">
    <style>
        #preview-data code { background-color: #353535; color: #f5f5f5 }
    </style>
    ```
    mysql> SELECT * 
        -> FROM example_data.demo_data.home_rentals
        -> LIMIT 10;
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
    10 rows in set (0.36 sec)
    ```
    </div>

1. Now you are ready to create your first predictor.  Use the `CREATE PREDICTOR` syntax by copy and pasting this command into your SQL client:

    ```
    CREATE PREDICTOR mindsdb.home_rentals_predictor
    FROM example_data
    	(SELECT * FROM demo_data.home_rentals)
    PREDICT rental_price;
    ```
    <div id="create-predictor">
      <style>
        #create-predictor code { background-color: #353535; color: #f5f5f5 }
      </style>
    ```
    mysql> CREATE PREDICTOR mindsdb.home_rentals_predictor
        -> FROM example_data
        -> (SELECT * FROM demo_data.home_rentals)
        -> PREDICT rental_price;
    Query OK, 0 rows affected (9.79 sec)
    ```
    </div>

1. It may take a couple of minutes for training to complete.  You can monitor the status of your predictor by copy and pasting this command into your SQL client:

    ```
    SELECT status
    FROM mindsdb.predictors
    WHERE name='home_rentals_predictor';
    ```
    <div id="predictor-status">
      <style>
        #predictor-status code { background-color: #353535; color: #f5f5f5 }
      </style>
    ```
    mysql> SELECT status
        -> FROM mindsdb.predictors
        -> WHERE name='home_rentals_predictor';
    +----------+
    | status   |
    +----------+
    | training |
    +----------+
    1 row in set (0.19 sec)
    ...
    mysql> SELECT status
        -> FROM mindsdb.predictors
        -> WHERE name='home_rentals_predictor';
    +----------+
    | status   |
    +----------+
    | complete |
    +----------+
    1 row in set (0.31 sec)
    ```
    </div>

1. You should see a status of `generating`, then `training` initially, followed by `complete` once it is done.  The [`SELECT` syntax](/sql/api/select) will allow you to make a prediction based on features.  Make your first prediction by copy and pasting this command into your SQL client:

    ```
    SELECT rental_price
    FROM mindsdb.home_rentals_predictor
    WHERE number_of_bathrooms=2 AND sqft=1000;
    ```
    <div id="make-prediction">
      <style>
        #make-prediction code { background-color: #353535; color: #f5f5f5 }
      </style>
    ```
    mysql> SELECT rental_price
        -> FROM mindsdb.home_rentals_predictor
        -> WHERE number_of_bathrooms=2 AND sqft=1000;
    +--------------+
    | rental_price |
    +--------------+
    | 1130         |
    +--------------+
    1 row in set (0.38 sec)
    ```
    </div>
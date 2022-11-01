This simple documentation should guide over how to bring ML capabilities to clickhouse. The idea is that you should be able to treat ML models just as if they were normal Clickhouse tables. What we have built so far allows you to create, train models, and finally query such models straight from the database.


## How can you try it?

We are trying to make it as simple as possible, what we have now is 3 simple steps:

1. Install mindsdb which is a python based server that deals with the ML part: `pip3 install mindsdb`

2. Then you simply run the server:  `python3 -m mindsdb`



## Hands on ML  

---
## 1. Connect MindsDB to a MySQL Client

You can use the MindsDB SQL Editor or open your preferred MySQL client and connect it to MindsDB.

=== "Using the MindsDB SQL Editor"
    Just log in to your account, and you will be automatically directed to the  [Editor](https://cloud.mindsdb.com/editor).

=== "Connecting to a Third-party MySQL Client"
    To connect to MindsDB from another SQL client use `cloud.mindsdb.com` as a host, `3306` port and your MindsDB Cloud credentials for username/password.
    ```txt
      "user":[your_mindsdb_cloud_username],
      "password:"[your_mindsdb_cloud_password]",
      "host":"cloud.mindsdb.com",
      "port":"3306"
    ```

    !!! Tip ""
        If you do not already have a preferred SQL client, we recommend [DBeaver Community Edition](https://dbeaver.io/download/).

---

## 2. Connecting a Database [`#!sql CREATE DATABASE`](/sql/create/databases/)

For this quickstart, we have already prepared some example data for you.  To add it to your account, use the [`#!sql CREATE DATABASE`](/sql/create/databases/) syntax by copying and pasting this command into your SQL client:


```sql
CREATE DATABASE example_data
WITH ENGINE = "postgres",
PARAMETERS = { 
  "user": "demo_user",
  "password": "demo_password",
  "host": "3.220.66.106",
  "port": "5432",
  "database": "demo"
}
```

On execution, you should get:

```sql
Query OK, 0 rows affected (3.22 sec)
```

---

## 3. Previewing Available Data

You can now preview the available data with a standard `#!sql SELECT`. To preview the Home Rentals dataset, copy and paste this command into your SQL client:

```sql 
SELECT * 
FROM example_data.demo_data.home_rentals
LIMIT 10;
```

On execution, you should get:

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

---

## 4. Creating a Predictor [`#!sql CREATE MODEL`](/sql/create/predictor/)

Now you are ready to create your first predictor. Use the [`#!sql CREATE MODEL`](/sql/create/predictor/) syntax by copying and pasting this command into your SQL client:

```sql 
CREATE MODEL mindsdb.home_rentals_predictor
FROM example_data
  (SELECT * FROM demo_data.home_rentals)
PREDICT rental_price;
```

```sql
Query OK, 0 rows affected (9.79 sec)
```

---

## 5. Checking the Status of a Predictor

A predictor may take a couple of minutes for the training to complete. You can monitor the status of your predictor by copying and pasting this command into your SQL client:

```sql
SELECT status
FROM mindsdb.models
WHERE name='home_rentals_predictor';
```

On execution, you should get:

```sql
+----------+
| status   |
+----------+
| training |
+----------+
```
Or:

```sql
+----------+
| status   |
+----------+
| complete |
+----------+
```

!!! attention "Predictor Status Must be 'complete' Before Making a Prediction"

---

## 6. Making a Prediction via [`#!sql SELECT`](/sql/api/select/)

The [`SELECT`](/sql/api/select/) syntax will allow you to make a prediction based on features.  Make your first prediction by copying and pasting this command into your SQL client:

```sql 
SELECT rental_price
FROM mindsdb.home_rentals_predictor
WHERE number_of_bathrooms=2 AND sqft=1000;
```

On execution, you should get:

```sql
+--------------+
| rental_price |
+--------------+
| 1130         |
+--------------+
```

!!! done "Congratulations"
      If you got this far, you have trained a predictive model using SQL and have used it to tell the future!

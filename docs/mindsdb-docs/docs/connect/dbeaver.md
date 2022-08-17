# Connect to MindsDB from DBeaver

DBeaver is a database tool that allows you to connect to and work with various database engines. You can download it [here](https://dbeaver.io/).

## Data Setup

First, you create a new database connection in DBeaver by clicking the icon, as shown below.

![New Database Connection](/assets/sql/dbeaver_1.png)

Next, choose the MySQL database engine and click the *Next* button.

![Choose database engine](/assets/sql/dbeaver_2.png)

Now it's the time to fill in the connection details.

![Connection details](/assets/sql/dbeaver_3.png)

There are two options as follows:

- You can connect to your MindsDB Cloud account. To do that, please use the connection details below:
    - Hostname: `cloud-mysql.mindsdb.com`
    - Port: `3306`
    - Username: *your MindsDB Cloud username*
    - Password: *your MindsDB Cloud password*
    - Database: *leave it empty*

- You can connect to your local MindsDB. To do that, please use the connection details below:
    - Hostname: `127.0.0.1`
    - Port: `47334`
    - Username: *mindsdb*
    - Password: *leave it empty*
    - Database: *leave it empty*

Now we are ready to test the connection.

## Testing the Connection

Click on the `Test Connection...` button to check if all the provided data allows you to connect to MindsDB

![Connection test](/assets/sql/dbeaver_4.png)

On success, you should see the message, as shown above.

## Let's Run Some Queries

To finally make sure that our MinsdDB database connection works, let's run some queries.

```sql
SELECT *
FROM mindsdb.predictors;
```

On execution, we get:

```sql
+-------------------------+--------+------------------+------------------+-------------+---------------+-----+-----------------+----------------+
|name                     |status  |accuracy          |predict           |update_status|mindsdb_version|error|select_data_query|training_options|
+-------------------------+--------+------------------+------------------+-------------+---------------+-----+-----------------+----------------+
|house_sales_model        |complete|0.4658770134240238|ma                |up_to_date   |22.7.5.1       |     |                 |                |
|process_quality_predictor|complete|1.0               |silica_concentrate|up_to_date   |22.7.5.1       |     |                 |                |
|home_rentals_model       |complete|0.9991920992432087|rental_price      |up_to_date   |22.7.4.0       |     |                 |                |
+-------------------------+--------+------------------+------------------+-------------+---------------+-----+-----------------+----------------+
```

Here is how it looks in DBeaver:

![Query test](/assets/sql/dbeaver_5.png)

!!! tip "Whitelist MindsDB Cloud IP address"
    If you need to whitelist MindsDB Cloud IP address to have access to your database, reach out to MindsDB team so we can share the Cloud static IP with you.

!!! tip "What is next?"
    We recommend you to follow one of our tutorials or learn more about the [MindsDB Database](/sql/table-structure/).

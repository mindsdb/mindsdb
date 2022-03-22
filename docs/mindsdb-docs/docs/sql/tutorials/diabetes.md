<div style="float:right;" ><img src="/assets/tutorials/diabetes/pg4admin/images.png" width="200" height="150" /></div><div> <img src="/assets/tutorials/diabetes/pg4admin/diabetes_logo.png" style="float:left;" width="50" height="50" /><h1><strong>Diabetes</strong></h1></div>

*Dataset:[Diabetes Data](https://github.com/mindsdb/mindsdb-examples/blob/a43f66f0250c460c0c4a0793baa941307b09c9f2/others/diabetes_example/dataset/diabetes-train.csv)*

Communtiy Author: [Chandre Tosca Van Der Westhuizen](https://github.com/chandrevdw31)

Diabetes is a metabolic disease that causes high blood sugar and if left untreated can damage your nerves, eyes, kidneys and other organs. It is known as a silent killer, as recent studies have shown that by the year 2040 the world's diabetic patients will reach 642 million. The need to analyse vast medical data to assist in the diagnoses, treatment and management of illnesses is increasing for the medical community. With the rapid development of machine learning, it has been applied to many aspects of medical health and is transforming the health care system.

The vitality to intelligently transform information into valuable knowledge through machine learning has become more present in biosciences. With the use of predictive models, MindsDB can assist in classifying diabetic and non-diabetic patients or those who pose a high risk. This is just a small showcase on how MindsDB's machine learning will be able to assist in vastly enhancing the reach of illnesses, thereby making it more efficient and can revolutionize businesses and most importantly the health care system.

In this tutorial we will be exploring how we can use a machine learning model to classify negative and positive cases for diabetes.
**MindsDB** allows you to train your model from CSV data directly, however for this tutorial you will:

1. Create & setup a new database.
2. Load sample data into a table using the PGAdmin GUI Tool.
3. Allow connections to the database using Ngrok.
4. Setup a MindsDB connection to your local instance.
5. Create a predictor using SQL.

## Pre-requisites

For this tutorial, Docker is highly recommended. A `docker-compose.yml` file will be provided to get you started quickly.

To ensure you can complete all the steps, make sure you have access to the following tools:

1. A MindsDB instance. Check out the installation guide for [Docker](https://docs.mindsdb.com/deployment/docker/) or [PyPi](https://docs.mindsdb.com/deployment/pypi/). You can also use [MindsDB Cloud](https://docs.mindsdb.com/deployment/cloud/).
2. Optional: A PostgreSQL Database. You can install it [locally](https://www.postgresql.org/download/) or through [Docker](https://hub.docker.com/_/postgres).
3. Downloaded the dataset. You can get it from [here](https://github.com/mindsdb/mindsdb-examples/blob/a43f66f0250c460c0c4a0793baa941307b09c9f2/others/diabetes_example/dataset/diabetes-train.csv)
4. Access to PGAdmin4 (provided with the docker-compose file).
5. Optional: Access to ngrok. You can check the installation details at the [ngrok website](https://ngrok.com/).

## Docker

1. Create a new project directory named e.g. `mindsdb-tutorial`
2. Inside your project directory:

    2.1 Create a new file named `docker-compose.yml`.

    2.2 Create another directory named `data`.
  
    2.3 Download the CSV datafile and store it in your directory

Open the `docker-compose.yml` file with any text-editor, copy the following and save.

```docker

version: '3.5'

services:
  postgres:
    container_name: postgres_container
    image: postgres
    environment:
      POSTGRES_USER: ${POSTGRES_USER:-postgres}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:-changeme}
      PGDATA: /data/postgres
    volumes:
       - postgres:/data/postgres
    ports:
      - "5432:5432"
    network_mode: bridge
    restart: unless-stopped
  
  pgadmin:
    container_name: pgadmin_container
    image: dpage/pgadmin4
    environment:
      PGADMIN_DEFAULT_EMAIL: ${PGADMIN_DEFAULT_EMAIL:-pgadmin4@pgadmin.org}
      PGADMIN_DEFAULT_PASSWORD: ${PGADMIN_DEFAULT_PASSWORD:-admin}
      PGADMIN_CONFIG_SERVER_MODE: 'False'
    volumes:
       - pgadmin:/var/lib/pgadmin
       # Mounts local dir to docker container
       - ./data:/home

    ports:
      - "5050:80"
    network_mode: bridge
    restart: unless-stopped

volumes:
    postgres:
    pgadmin:


```

This compose stack features both a Postgres database with PGAdmin4 running on ports 5432 and 5050 respectively.
For the database to connect to MindsDB we will use a tunneling program Ngrok to allow the remote database connection without exposing your public IP.

Data will be loaded from a csv file using the import tool.
[Docker Volumes](https://docs.docker.com/storage/volumes/) are used for persistent storage of data.

To run, open a terminal in the project folder and run `docker-compose up -d` .
If you do not have the Docker images locally, it will first download them from docker hub and might take.

## Setup the database

After docker has started all the services
In this section, you will create a Postgres database and a table into which you will then load the dataset.

First, connect to your Postgres instance. There are cli options like [pgcli](https://www.pgcli.com/).

We will use a gui manager; **pgAdmin**

> The required details are specified within the compose file. Remember to change the username & password if you have a different one set up in.

1. Open your browser and enter localhost:5050 in the Navigation bar.
2. Setup or enter a master password.
![Access pg4admin](/assets/tutorials/diabetes/pg4admin/Accessing_pg4admin.gif)
3. Create a new server connection by navigating to the right side of the page and right click on 'Server', select 'Create' and enter the required details.

    3.1 For the purpose of the exercise, under the General Tab the Name entered is MIndsDB and the host name/address is localhost under the Connection tab.
![Create Server](/assets/tutorials/diabetes/pg4admin/Creating_pgserver.gif)

4. To create a new database, select the dropdown of the Server and navigate to 'Databases', right click on it and select 'Create' and then 'Databases'.Enter the required details.

    4.1 For this exercise, the Database was given the name DIABETES_DATA under the General tab.
![Create Database](/assets/tutorials/diabetes/pg4admin/Creating_pgdatabase.gif)

5. Select the dropdown of the database created, navigate to 'Schema' and right click on it to select 'Create' and then 'Schema'. Enter the required details.

6. Select the dropdown of the schema created and navigate to 'Tables' to right click on it and select 'Query Tool'. This will allow you to add a table with SQL code.The below code was used to create the table.

```sql
CREATE TABLE Diabetes (
    Number_of_times_pregnant INT ,
    Plasma_glucose_concentration DECIMAL(6,3) ,
    Diastolic_blood_pressure DECIMAL(6,3) ,
    Triceps_skin_fold_thickness DECIMAL(6,3) ,
    Two_Hour_serum_insulin DECIMAL(6,3) ,
    Body_mass_index DECIMAL(6,3) ,
    Diabetes_pedigree_function DECIMAL(6,3) ,
    Age INT ,
    Class varchar(100)
);
```

![Create table](/assets/tutorials/diabetes/pg4admin/Create_pgtable.gif)

**7. Import Data**

Before you import the data, please delete the first row as it is a table header. To import rows into your table, right click on the table name and select *Import/Export*.

- Under the Options tab, ensure the slider is set to *Import*.
 
- Select your file name and ensure that the delimiters are set.

![Import rows](/assets/tutorials/diabetes/pg4admin/Import_rows.gif)

**8. Select the data** 

By making a select query, the data that has been loaded can be verified.

  - Navigate to the table name to right click and select 'SCRIPTS', then choose 'SELECT scripts'.

  - Run the below query and see the results returned.

```sql
    SELECT * FROM "DIABETES_DATA".diabetes;
```

You can use the below query structure to show your database

```sql
    SELECT * FROM "Schema_name".table_name;
```
![Select tables](/assets/tutorials/diabetes/pg4admin/select_pgtable.gif)
All the rows imported will be retrieved.

Congratulations! You have completed setting up a Postgres database.

## Connect your database to MindsDB GUI

Now we are ready to connect your database to MindsDB's GUI. For this exercise, we will be connecting to MindsDB Cloud as it is much more convenient for most users to use.

To make our database available to MindsDB, we will run `ngrok` as our database instance is local.

The following command can be run in docker or a terminal on your device to set up a ngrok tunnel.

```bash
ngrok tcp [db-port]
```
For this example the port number used is 5432.

You should see a similar output:


```console
Session Status                online
Account                       myaccount (Plan: Free)
Version                       2.3.40
Region                        United States (us)
Web Interface                 http://127.0.0.1:4040
Forwarding                    tcp://x.tcp.ngrok.io:15076 -> localhost:5432
```

The information required is by the forwarded address, next to 'Forwarding' select and copy `x.tcp.ngrok.io:15076`  Once you have copied the information, you can add it to the information requested by the MindsDB GUI which we will get to in a moment.

For the next steps we will log into the MindsDB cloud interface. MindsDB Cloud is perfect if you are unable to install MindsDB on your device. If you are not registered yet, feel free to follow the below guide. If you have already registered, skip the next steps to connect your database.

### MindsDB Studio Cloud Set Up

You can visit this [link](https://docs.mindsdb.com/deployment/cloud/) and follow the steps in registering a MindsDB Cloud account.

On the landing page, navigate to the left corner of the screen and select `ADD DATABASE`. Enter the required details. Example is below

![MindsDB integration details](/assets/tutorials/diabetes/Connecting_database_to_MindsdbCloud.gif)

Click `Connect`, you should now see your Postgres database connection in the main screen.

You are now done with connecting MindsDB to your database! 🚀

## Create a predictor

Now we are ready to create our own predictor! We will start by using the MySQL API to connect to MindsDB and with a single SQL command create a predictor.

The predictor we will create will be trained to determine negative and positive cases for diabetes. Predictors are great machine learning models when working with large datasets and optimal for determining classifications.

Using the following command, you will connect through the MySQL API to MindsDB.
> Please note that the username and password to use will be the credentials you registered your MindsDB account with.

```bash
mysql -h cloud.mindsdb.com --port 3306 -u cloudusername@mail.com -p
```

![MindsDB mysql client connection](/assets/tutorials/diabetes/connecting_mysql_client.gif)

If you are successfully connected, make sure you connect to MindsDB's database.

```sql
USE mindsdb;
```

Use the following query to create a predictor that will predict the `class` (*positive or negative*) for the specific field parameters.

```sql
CREATE PREDICTOR mindsdb.diabetes_predictor
FROM Diabetes (
    SELECT * FROM "DIABETES_DATA".diabetes
) PREDICT class;
```

After creating the predictor you should see a similar output:

```console
Query OK, 0 rows affected (8.09 sec)
```

The predictor was created successfully and has started training. To check the status of the model, use the below query.

```sql
SELECT * FROM mindsdb.predictors WHERE name='diabetes_predictor';
```

After the predictor has finished training, you will see a similar output. Note that MindsDB does model testing for you automatically, so you will immediately see if the predictor is accurate enough.


```console
mysql> SELECT * FROM mindsdb.predictors WHERE name='diabetes_predictor';
+--------------------+----------+--------------------+---------+---------------+-----------------+-------+-------------------+------------------+
| name               | status   | accuracy           | predict | update_status | mindsdb_version | error | select_data_query | training_options |
+--------------------+----------+--------------------+---------+---------------+-----------------+-------+-------------------+------------------+
| diabetes_predictor | complete | 0.6546310832025117 | class   | up_to_date    | 2.61.0          | NULL  |                   |                  |
+--------------------+----------+--------------------+---------+---------------+-----------------+-------+-------------------+------------------+
1 row in set (0.57 sec)

```

The predictor has completed its training, indicated by the status column, and shows the accuracy of the model.
You can revisit training new predictors to increase accuracy by changing the query to better suit the dataset i.e. omitting certain columns etc.

Good job! We have successfully created and trained a predictive model ✨

## Make predictions

In this section you will learn how to make predictions using your trained model.
Now we will use the trained model to make predictions using a SQL query

Use the following query using mock data with the predictor.


```sql
SELECT class
FROM mindsdb.diabetes_predictor
WHERE when_data='{"number_of_times_pregnant": 0, "plasma_glucose_concentration": 135.0, "diastolic_blood_pressure": 65.0, "triceps_skin_fold_thickness": 30, "two_Hour_serum_insulin": 0, "body_mass_index": 23.5, "diabetes_pedigree_function": 0.366, "age": 31}'\G
```
The result:
```console
class: negative
1 row in set (0.32 sec)

*************************** 1. row ***************************

```

Viola! We have successfully created and trained a model and make our own prediction. How easy and amazing is MindsDB? 🎉

Want to try it out for yourself? Sign up for a [free MindsDB account](https://cloud.mindsdb.com/signup?utm_medium=community&utm_source=ext.%20blogs&utm_campaign=blog-crop-detection) and join our community!
Engage with MindsDB community on [Slack](https://join.slack.com/t/mindsdbcommunity/shared_invite/zt-o8mrmx3l-5ai~5H66s6wlxFfBMVI6wQ) or [Github](https://github.com/mindsdb/mindsdb/discussions) to ask questions, share and express ideas and thoughts!

For more check out other [tutorials and MindsDB documentation](https://docs.mindsdb.com/).

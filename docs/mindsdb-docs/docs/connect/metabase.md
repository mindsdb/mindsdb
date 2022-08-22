# MindsDB and Metabase

[Metabase](https://www.metabase.com/) is the easy, open source way to help everyone in your company work with data like an analyst. It lets you visualize your data easily and intuitively. Now that MindsDB supports the MySQL binary protocol, you can connect it to Metabase and train and explore there your models.

!!! warning "For the time being, only local and on premise installations are stable." 

## Set up both platforms

Refer to these guides to get both platforms up and running:

- MindsDB: head over the **Deployment** section on the left hand side navigation and choose one of the approaches listed there. Recall that MindsDB Cloud is not supported yet.  
- Metabase: [Metabase Open Source Edition](https://www.metabase.com/start/oss/)

In this guide, we will be using the [docker approach](/setup/self-hosted/docker/) for MindsDB and the [jar approach](https://www.metabase.com/docs/latest/installation-and-operation/running-the-metabase-jar-file.html) for Metabase.

## Connect MindsDB in Metabase

Follow the steps below to connect together your MindsDB & Metabase installations.

In metabase: 

- Navigate to the *Admin settings* by clicking the cog in the bottom left corner.  
- Once there, in the navigation top bar, click on *Databases*  
- Click on *Add database* in top right corner  
- Fill the following data in the form:  
    - Database type: MySQL  
    - Display name: MindsDB    
    - Host: localhost  
    - Port: 47335  
    - Database name: mindsdb  
    - Username: mindsdb  
    - Password: *leave it empty*  

<img src="/assets/metabase_add_database.png" />

- Click on save

Now you're connected!

<img src="/assets/metabase_connected.png" />

## Testing that everything works

Now that you have the connection, most of the SQL statements that you usually perform on your [MindsDB SQL Editor](/connect/mindsdb_editor/) can be run on Metabase. For example, let's try something easy:

- In your Metabase's home page, click on *New > SQL query* in the top right corner.  
- Select your MindsDB database
- In the editor type the following

```sql
SHOW TABLES;
```

<img src="/assets/metabase_run_query_show_tables.png" />


However, creating a [database connection](sql/tutorials/home-rentals/#connecting-the-data) will fail as `{}` are used by JDBC as escape sequences.

```sql
CREATE DATABASE example_db
    WITH ENGINE = "postgres",
    PARAMETERS = {
        "user": "demo_user",
        "password": "demo_password",
        "host": "3.220.66.106",
        "port": "5432",
        "database": "demo"
};
```

<img src="/assets/metabase_run_query_failure.png" />


Still, once above table is created, for example through the [MindsDB SQL Editor](/connect/mindsdb_editor/), running queries against that database should be fine:

```sql
SELECT * 
FROM example_db.demo_data.home_rentals 
LIMIT 10;
```


<img src="/assets/metabase_run_query_home_rentals.png" />

## What's Next?

Now that you are all set, we recommend you check out our **Tutorials** and **Community Tutorials** sections, where you'll find various examples of regression, classification, and time series predictions with MindsDB.

To learn more about MindsDB itself, follow the guide on [MindsDB database structure](/sql/table-structure/). Also, don't miss out on the remaining pages from the **SQL API** section, as they explain a common SQL syntax with examples.

Have fun!

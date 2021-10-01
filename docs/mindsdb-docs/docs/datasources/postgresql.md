# Connect to PostgreSQL database

Connecting MindsDB to your PostgreSQL database can be done in two ways:

* Using [MindsDB Studio](#mindsdb-studio).
* Using [psql client](#psql-client).

## Prerequisite

To connect to your PostgreSQL Server from MindsDB, you will need to install the MySQL foreign data wrapper for PostgreSQL.

!!! Tip "How to install the MySQL foreign data wrapper"
    * Please check the mysql_fdw <a href="https://github.com/EnterpriseDB/mysql_fdw#installation" target="_blank">documentation</a>.
    * Stackoverflow <a href="https://stackoverflow.com/questions/24683035/setup-mysql-foreign-data-wrapper-in-postgresql" target="_blank">answers</a>.

## MindsDB Studio

Using MindsDB Studio, you can connect to the PostgreSQL database with a few clicks.

#### Connect to database

1. From the left navigation menu, select Database Integration.
2. Click on the `ADD DATABASE` button.
3. In the `Connect to Database` modal:
    1. Select PostgreSQL as the Supported Database.
    2. Add the Database name.
    3. Add the Hostname.
    4. Add Port.
    5. Add PostgreSQL user.
    6. Add Password for PostgreSQL user.
    7. Click on `CONNECT`.


![Connect to PostgreSQL](/assets/data/postgresql.gif)

#### Create new Datasource

1. Click on the `NEW DATASOURCE` button.
2. In the `Datasource from DB integration` modal:
    1. Add Datasource Name.
    2. Add Database name.
    3. Add SELECT Query (e.g. SELECT * FROM my_database)
    4. Click on `CREATE`.

![Create PostgreSQL Datasource](/assets/data/postgresql-ds.gif)

!!! Success "That's it:tada: :trophy:  :computer:"
    You have successfully connected to PostgreSQL from MindsDB Studio. The next step is to train the [Machine Learning model](/model/train).

## PSQL client

!!! Info "How to extend MindsDB configuration"
    Our suggestion is to always use [MindsDB Studio](/datasources/postgresql/#mindsdb-studio) to connect MindsDB to your database. If you still want to extend the configuration without using MindsDB Studio follow [configuration example](/datasources/configuration/#postgresql-configuration).


After creating the required configuration, you will need to start MindsDB and provide the path to the newly created `config.json`:

```
python3 -m mindsdb --api=http,mysql --config=config.json
```

The `--api` parameter specifies the type of API to use, in this case HTTP and MySQL. The `--config` parameter specifies the location of the configuration file.

![Start MindsDB with config](/assets/data/start-config.gif)

If MindsDB is successfully connected to your PostgreSQL database, it will create a new schema `mindsdb` and new table `predictors`.
After starting the server, you can run a `SELECT` query from your psql-client to make sure integration has been successful.

```sql
SELECT * FROM mindsdb.predictors;
```

![SELECT from MindsDB predictors table](/assets/data/psql-select.gif)

!!! Success "That's it :tada: :trophy:  :computer:"
    You have successfully connected MindsDB Server and PostgreSQL. The next step is to [train the Machine Learning model](/model/postgresql).


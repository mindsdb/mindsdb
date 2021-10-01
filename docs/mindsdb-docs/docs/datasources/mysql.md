# Connect to MySQL database

Connecting MindsDB to your MySQL database can be done in two ways:

* Using [MindsDB Studio](#mindsdb-studio).
* Using [MySQL client](#mysql-client).

## Prerequisite

To connect to your MySQL Server from MindsDB, you will need to enable the FEDERATED Storage Engine.

!!! Tip "How to enable the FEDERATED Storage Engine"
    * Please check the MySQL <a href="https://dev.mysql.com/doc/refman/8.0/en/federated-storage-engine.html" target="_blank">documentation</a>.
    * Stackoverflow <a href="https://stackoverflow.com/questions/5210309/how-can-i-enable-federated-engine-in-mysql-after-installation" target="_blank">answers</a>.
    * Read more about the <a href="https://dev.mysql.com/doc/refman/8.0/en/federated-description.html" target="_blank">FEDERATED Storage Engine</a>.

## MindsDB Studio

Using MindsDB Studio, you can connect to the MySQL database with a few clicks.

#### Connect to database

1. From the left navigation menu, select Database Integration.
2. Click on the `ADD DATABASE` button.
3. In the `Connect to Database` modal window:
    1. Select MySQL as the Supported Database.
    2. Add the Database name.
    3. Add the Hostname.
    4. Add Port.
    5. Add MySQL user.
    6. Add Password for MySQL user.
    7. Click on `CONNECT`.


![Connect to MySQL](/assets/data/mysql.gif)

#### Create new Datasource

1. Click on the `NEW DATASOURCE` button.
2. In the `Datasource from DB integration` modal window:
    1. Add Datasource Name.
    2. Add Database name.
    3. Add SELECT Query (e.g. SELECT * FROM my_database)
    4. Click on `CREATE`.

![Create MySQL Datasource](/assets/data/mysql-ds.gif)

!!! Success "That's it :tada: :trophy:  :computer:"
    You have successfully connected to MySQL from MindsDB Studio. The next step is to train the [Machine Learning model](/model/train).

## MySQL client

!!! Info "How to extend MindsDB configuration"
    Our suggestion is to always use [MindsDB Studio](/datasources/mysql/#mindsdb-studio) to connect MindsDB to your database. If you still want to extend the configuration without using MindsDB Studio follow the [configuration example](/datasources/configuration/#mysql-configuration)


After adding the required configuration, you will need to start MindsDB and provide the path to the newly created `config.json`:

```
python3 -m mindsdb --api=http,mysql --config=config.json
```

The `--api` parameter specifies the type of API to use, in this case HTTP and MySQL. The `--config` parameter specifies the location of the configuration file.

![Start MindsDB with config](/assets/data/start-config.gif)

If MindsDB is successfully connected to your MySQL database, it will create a new database `mindsdb` and new table `predictors`.
After starting the server, you can run a `SELECT` query from your mysql-client to make sure integration has been successful.

```sql
SELECT * FROM mindsdb.predictors;
```

![SELECT from MindsDB predictors table](/assets/data/mysql-select.gif)

!!! Success "That's it :tada: :trophy:  :computer:"
    You have successfully connected MindsDB Server and MySQL. The next step is to [train the Machine Learning model](/model/mysql).


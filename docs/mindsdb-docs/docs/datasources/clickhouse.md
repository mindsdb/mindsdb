# Connect to ClickHouse database

Connecting MindsDB to your ClickHouse database can be done in two ways:

* Using [MindsDB Studio](#mindsdb-studio).
* Using [ClickHouse client](#clickhouse-client).

## MindsDB Studio

Using MindsDB Studio, you can connect to the ClickHouse database with a few clicks.

#### Connect to database

1. From the left navigation menu, select Database Integration.
2. Click on the `ADD DATABASE` button.
3. In the `Connect to Database` modal window:
    1. Select ClickHouse as Supported Database.
    2. Add the Database name.
    3. Add the Hostname.
    4. Add Port.
    5. Add ClickHouse user.
    6. Add Password for ClickHouse user.
    7. Click on `CONNECT`.


![Connect to ClickHouse](/assets/data/clickhouse.gif)

#### Create new Datasource

1. Click on the `NEW DATASOURCE` button.
2. In the `Datasource from DB integration` modal window:
    1. Add Datasource Name.
    2. Add Database name.
    3. Add SELECT Query e.g (SELECT * FROM my_database)
    4. Click on `CREATE`.

![Create ClickHouse Datasource](/assets/data/clickhouse-ds.gif)

!!! Success "That's it :tada: :trophy:  :computer:"
    You have successfully connected to ClickHouse from MindsDB Studio. The next step is to train the [Machine Learning model](/model/train).

## ClickHouse client

!!! Info "How to extend MindsDB configuration"
    Our suggestion is to always use [MindsDB Studio](/datasources/clickhouse/#mindsdb-studio) to connect MindsDB to your database. If you still want to extend the configuration without using MindsDB Studio follow the [configuration example](/datasources/configuration/#clickhouse-configuration).

After adding the required configuration, you will need to start MindsDB and provide the path to the newly created `config.json`:

```
python3 -m mindsdb --api=http,mysql --config=config.json
```

The `--api` parameter specifies the type of API to use -- in this case HTTP and MySQL. The `--config` parameter specifies the location of the configuration file.

![Start MindsDB with config](/assets/data/start-config.gif)

If MindsDB is successfully connected to your ClickHouse database, it will create a new database `mindsdb` and new table `predictors`.
After starting the server, you can run a `SELECT` query from your ClickHouse client to make sure integration has been successful.

```sql
SELECT * FROM mindsdb.predictors;
```

![SELECT from MindsDB predictors table](/assets/data/clickhouse-select.gif)

!!! Success "That's it :tada: :trophy:  :computer:"
    You have successfully connected MindsDB Server and ClickHouse. The next step is to [train the Machine Learning model](/model/clickhouse).


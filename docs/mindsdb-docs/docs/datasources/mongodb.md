# Connect to MongoDB database

Connecting MindsDB to MongoDB can be done in two ways:

* Using [MindsDB Studio](#mindsdb-studio).
* Using [Mongo clients](#mongo-shell).

The current integration works by accessing MongoDB through MindsDB MongoDB API as a new datasource.

![MindsDB-MongoDB](/assets/databases/mongodb/mongo-mdb-current.png)

The new version of MindsDB will allow direct integration inside MongoDB.

![MindsDB-MongoDB](/assets/databases/mongodb/mongo-mdb.png)


## MindsDB Studio

Using MindsDB Studio, you can connect to the MongoDB with a few clicks.

#### Connect to database

1. From the left navigation menu, select Database Integration.
2. Click on the `ADD DATABASE` button.
3. In the `Connect to Database` modal window:
    1. Select MongoDB as the Supported Database.
    2. Add Host (connection string)
    3. Add Port
    4. Add Username
    5. Add Password
    6. Click on `CONNECT`


![Connect to MongoDB](/assets/data/mongo/mongo.gif)

#### Create new dataset

1. Click on the `NEW DATASET` button.
2. In the `Datasource from DB integration` modal window:
    1. Add Datasource Name
    2. Add Database name
    3. Add Collection name
    3. Add find query to select documents from the collection(must be valid JSON format)
    4. Click on `CREATE`.

![Create Mongodb Datasource](/assets/data/mongo/mongo-ds.gif)

!!! Success "That's it :tada: :trophy:  :computer:"
    You have successfully connected to MongoDB from MindsDB Studio. The next step is to train the [Machine Learning model](/model/train).


## Mongo shell


!!! Info "How to extend MindsDB configuration"
    Our suggestion is to always use [MindsDB Studio](/datasources/mariadb/#mindsdb-studio) to connect MindsDB to your database. If you still want to extend the configuration without using MindsDB Studio follow the steps in [configuration example](/datasources/configuration/#mongodb-configuration).


After adding the required configuration, you will need to start MindsDB and provide the path to the newly created `config.json`:

```
python3 -m mindsdb --api=http,mongodb --config=config.json
```

The `--api` parameter specifies the type of API to use -- in this case HTTP and Mongo. The `--config` parameter specifies the location of the configuration file.

![Start MindsDB with config](/assets/data/mongo/start-mongo.gif)


### Connect to MongoDB API

You can use mongo shell to connect to mindsdb's MongoDB API. As a prerequisite you need mongo shell version greather then 3.6. To connect use the `host` that you have specified inside the `api['mongodb']` key e.g:

```
mongo --host 127.0.0.1 -u "username" -p "password"
```

To make sure everything works, you should be able to list the predictors collection:

```
use mindsdb
show collections
```

![find mindsdb predictors collection](/assets/data/mongo/find-predictors.gif)

!!! Success "That's it :tada: :trophy:  :computer:"
    You have successfully connected MindsDB Server and MongoDB. The next step is to [train the Machine Learning model](/model/mongodb).


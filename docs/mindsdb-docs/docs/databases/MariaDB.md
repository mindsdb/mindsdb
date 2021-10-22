# AI Tables in MariaDB

Now, you can train machine learning models straight from the database by using MindsDB and [MariaDB](https://mariadb.org/).

![MindsDB-ClickHouse](/assets/databases/mdb-maria.png)

### Prerequisite

You will need MindsDB version >= 2.0.0 and MariaDB installed:

* [Install MindsDB](/Installing/)
* [Install MariaDB](https://mariadb.com/kb/en/getting-installing-and-upgrading-mariadb/)

### Configuration

!!! info "Default configuration"
    MindsDB will try to use the default configuration(hosts, ports, usernames) for each of the database integrations. If you want to extend that or you are using different parameters creata a new config.json file. 

The available configuration options are:

* api['http] -- This key is used for starting the MindsDB http server by providing:
    * host(default 0.0.0.0.) - The mindsdb server address.
    * port(default 47334) - The mindsdb server port.
* api['mysql'] -- This key is used for database integrations that works through MySQL protocol. The required keys are:
    * user(default root).
    * password(default empty).
    * host(default localhost).
    * port(default 47335).
    * log -- The logging configuration:
        * console_level - "INFO", "DEBUG", "ERROR".
        * file - Location of the log file.
        * file_level - "INFO", "DEBUG", "ERROR".
        * folder logs - Directory of log files.
        * format - Format of log message e.g "%(asctime)s - %(levelname)s - %(message)s".
* integrations -- This key specifies the integration type in this case `default_mariadb`. The required keys are:
    * user(default root) - The MariaDB user name.
    * host(default localhost) - Connect to the MariaDB server on the given host. 
    * password - The password of the MariaDB account. 
    * type - Integration type(mariadb, postgresql, mysql, clickhouse).
    * port(default 3306) - The TCP/IP port number to use for the connection. 
* interface -- This key is used by MindsDB and provides the path to the directory where MindsDB shall save configuration and model files:
    * datastore
        * enabled(default false) - If not provided MindsDB will use default storage inside /var.
        * storage_dir - Path to the storage directory for datastore.
    * mindsdb_native
        * enabled -  If not provided mindsdb_native will use default storage inside /var.
        * storage_dir - Path to the storage directory for datastore.

<details class="success">
    <summary> Configuration example</summary>  
```json
{
    "api": {
        "http": {
            "host": "0.0.0.0",
            "port": "47334"
        },
        "mysql": {
            "host": "127.0.0.1",
            "password": "",
            "port": "47335",
            "user": "root"
        }
    },
    "config_version": "1.3",
    "debug": true,
    "integrations": {
        "default_mariadb": {
           "enabled": true,
           "host": "localhost",
           "password": "password",
           "port": 3306,
           "type": "mariadb",
           "user": "root"
        }
    },
    "log": {
        "level": {
            "console": "DEBUG",
            "file": "INFO"
        }
    },
    "storage_dir": "/storage"
}
```
</details> 

!!! warning "Install CONNECT Storage Engine"
    Also you need to install the CONNECT Storage Engine to access external local data. Checkout [MariaDB docs](https://mariadb.com/kb/en/installing-the-connect-storage-engine/) on how to do that.


### Start MindsDB
To start mindsdb run following command:

```python
python3 -m mindsdb --api=mysql --config=config.json
```
The --api parameter specifies the type of API to use in this case mysql. 
The --config specifies the location of the configuration file. 

### Train new model

To train a new model, insert a new record inside the mindsdb.predictors table as:

```sql
INSERT INTO
   mindsdb.predictors(name, predict, select_data_query, training_options) 
VALUES
   ('used_cars_model', 'price', 'SELECT * FROM test.UsedCarsData', "option,value");
```

* name (string) -- The name of the predictor.
* predict (string) --  The feature you want to predict, in this example price. To predict multiple features include a comma separated string e.g 'price,year'.
* select_data_query (string) -- The SELECT query that will ingest the data to train the model.
* training_options (JSON as comma separated string) -- optional value that contains additional training parameters. For a full list of the parameters check the [PredictorInterface](/PredictorInterface/#learn).

### Query the model

To query the model and get the predictions SELECT the target variable, confidence and explanation for that prediction.

```sql
SELECT
   price AS predicted,
   price_confidence AS confidence,
   price_explain AS info 
FROM
   mindsdb.used_cars_model 
WHERE
   model = "A6" 
   AND mileage = 36203 
   AND transmission = "Automatic" 
   AND fuelType = "Diesel" 
   AND mpg = "64.2" 
   AND engineSize = 2 
   AND year = 2016 
   AND tax = 20;
```
You should get a similar response from MindsDB as:

| price  | predicted | info   |
|----------------|------------|------|
| 13111 | 0.9921 | Check JSON below  |

```json

{
    "predicted_value": 13111,
    "confidence": 0.9921,
    "prediction_quality": "very confident",
    "confidence_interval": [10792, 32749],
    "important_missing_information": [],
    "confidence_composition": {
        "Model": 0.009,
        "year": 0.013
    },
    "extra_insights": {
        "if_missing": [{
            "Model": 12962
        }, {
            "year": 12137
        }, {
            "transmission": 2136
        }, {
            "mileage": 22706
        }, {
            "fuelType": 7134
        }, {
            "tax": 13210
        }, {
            "mpg": 27409
        }, {
            "engineSize": 13111
        }]
    }
}

```
### Delete the model

To delete the predictor that you have previously created, you need to delete it from `mindsdb.predictors` table. The name should be equal to name added in the INSERT statement while creating the predictor, e.g:

```sql
DELETE FROM mindsdb.predictors WHERE name='used_cars_model'
```

### Train and predict multiple features

You can train a model that will predict multiple features by adding a comma separated features values in the predict column. e.g to predict the `price` and a `year`:

```sql
INSERT INTO
   mindsdb.predictors(name, predict, select_data_query, training_options) 
VALUES
   ('used_cars_model', 'price,year', 'SELECT * FROM test.UsedCarsData', "option,value"});
```
And query it using the `select_data_query`:

```sql
SELECT
   price AS predicted,
FROM
   mindsdb.used_cars_model 
WHERE
    select_data_query='SELECT year FROM price_data';
```

The requirements to query with `select_data_query` are:

* It must be a valid SQL statement
* It must return columns with names the same as predictor fields.

If you want to follow along with a tutorial check out [AI Tables in MariaDB](mindsdb.com/blog) tutorial.

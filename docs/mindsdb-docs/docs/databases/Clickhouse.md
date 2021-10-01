# AI Tables in ClickHouse

Now, you can train machine learning models straight from the database by using MindsDB and [ClickHouse](https://clickhouse.tech/).

![MindsDB-ClickHouse](/assets/clickhouse-mdb-diagram.png)


### Prerequisite

You will need MindsDB version >= 2.0.0 and ClickHouse installed.

* [Install MindsDB](/Installing/)
* [Install ClickHouse](https://clickhouse.tech/docs/en/getting-started/install/)

### Configuration

!!! info "Default configuration"
    MindsDB will try to use the default configuration(hosts, ports, usernames) for each of the database integrations. If you want to extend that or you are
    using different parameters create a new config.json file.

The avaiable configuration options are:

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
* integrations['default_clickhouse'] -- This key specifies the integration type in this case `default_clickhouse`. The required keys are:
    * user(default default) - The ClickHouse user name.
    * host(default localhost) - Connect to the ClickHouse server on the given host.
    * password - The password of the ClickHouse user.
    * type - Integration type(mariadb, postgresql, mysql, clickhouse).
    * port(default 8123) - The TCP/IP port number to use for the connection.
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
      "default_clickhouse": {
          "enabled": true,
          "host": "localhost",
          "password": "root",
          "port": 8123,
          "type": "mysql",
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
   ('airq_predictor', 'SO2', 'SELECT * FROM data.pollution_measurement', {"option": "value"});
```

* name (string) -- The name of the predictor.
* predict (string) --  The feature you want to predict, in this example SO2.
* select_data_query (string) -- The SELECT query that will ingest the data to train the model.
* training_options (JSON as string) -- optional value that contains additional training parameters. For a full list of the parameters check the [PredictorInterface](/PredictorInterface/#learn).

### Query the model

To query the model and get the predictions SELECT the target variable, confidence and explanation for that prediction.

```sql
SELECT
    SO2 AS predicted,
    SO2_confidence AS confidence,
    SO2_explain AS info
FROM airq_predictor
WHERE (NO2 = 0.005)
    AND (CO = 1.2)
    AND (PM10 = 5)
```
You should get a similar response from MindsDB as:

| price  | predicted | info   |
|----------------|------------|------|
| 0.001156540079952395 | 0.9869 | Check JSON bellow  |


```json
{
    "predicted_value": 0.001156540079952395,
    "confidence": 0.9869,
    "prediction_quality": "very confident",
    "confidence_interval": [0.003184904620383531, 0.013975553923630717],
    "important_missing_information": ["Station code", "Latitude", "O3"],
    "confidence_composition": {
        "CO": 0.006
    },
    "extra_insights": {
        "if_missing": [{
            "NO2": 0.007549311956155897
        }, {
            "CO": 0.005459383721227349
        }, {
            "PM10": 0.003870252306568623
        }]
    }
}
```
### Delete the model
If you want to delete the predictor that you have previously created run:

```sql
INSERT INTO mindsdb.commands values ('DELETE predictor airq_predictor');
```

To get additional information about the integration check out [Machine Learning Models as Tables with ClickHouse](https://www.mindsdb.com/blog/machine-learning-models-as-tables) tutorial.

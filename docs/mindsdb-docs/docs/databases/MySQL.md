# AI Tables in MySQL

Now, you can train machine learning models straight from the database by using MindsDB and [MySQL](https://www.mysql.com/).

![MindsDB-MySQL](/assets/databases/mdb-mysql.png)

### Prerequisite

You will need MindsDB version >= 2.3.0 and MySQL installed:

* [Install MindsDB](/Installing/)
* [Install MySQL](https://www.mysql.com/downloads/)
* [Enable FEDERATED Storage Engine](https://dev.mysql.com/doc/refman/8.0/en/federated-storage-engine.html)

### Configuration

!!! info "Default configuration"
    MindsDB will try to use the default configuration(hosts, ports, usernames) for each of the database integrations. If you want to extend that or you are using different parameters create a new config.json file. 

The available configuration options are:

* api['http] -- This key is used for starting the MindsDB http server by providing:
    * host(default 127.0.0.1) - The mindsdb server address.
    * port(default 47334) - The mindsdb server port.
* api['mysql'] -- This key is used for database integrations that works through MySQL protocol. The required keys are:
    * user(default root).
    * password(default empty).
    * host(default 127.0.0.1).
    * port(default 47335).
* integrations['default_mysql'] -- This key specifies the integration type in this case `default_mysql`. The required keys are:
    * user(default root) - The MySQL user name.
    * host(default 127.0.0.1) - Connect to the MySQL server on the given host. 
    * password - The password of the MySQL account. 
    * type - Integration type(mariadb, postgresql, mysql, clickhouse, mongodb).
    * port(default 3306) - The TCP/IP port number to use for the connection. 
    * enabled(true|false) - Enable MySQL integration.
* log['level'] -- The logging configuration(optional):
    * console - "INFO", "DEBUG", "ERROR".
    * file - Location of the log file.
* storage_dir -- The directory where mindsdb will store models and configuration.

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
       "default_mysql": {
            "enabled": true,
            "host": "localhost",
            "password": "root",
            "port": 3307,
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

!!! warning "Enable FEDERATED storage engine"
   The FEDERATED storage engine is not enabled by default in the running server; to enable FEDERATED, you must start the MySQL server binary using the --federated option. Check [official docs](https://dev.mysql.com/doc/refman/8.0/en/federated-storage-engine.html) for more info.


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
INSERT INTO mindsdb.predictors(name, predict, select_data_query) VALUES ('us_consumption', 'consumption', 'SELECT * FROM us_consumption');
```

* name (string) -- The name of the predictor.
* predict (string) --  The feature you want to predict, in this example consumption. To predict multiple features include a comma separated string e.g 'consumption,income'.
* select_data_query (string) -- The SELECT query that will ingest the data to train the model.
* training_options (JSON as comma separated string) -- optional value that contains additional training parameters. For a full list of the parameters check the [PredictorInterface](/PredictorInterface/#learn).

### Query the model

To query the model and get the predictions SELECT the target variable, confidence and explanation for that prediction.

```sql
SELECT
   consumption AS predicted,
   consumption_confidence AS confidence,
   consumption_explain AS info 
FROM
   mindsdb.us_consumption 
WHERE 
   when_data='{"income": 1.182497938, "production": 5.854555956,"savings": 3.183292657, "unemployment": 0.1, "t":"2020-01-02"}';
```
You should get a similar response from MindsDB as:

| consumption  | predicted | info   |
|----------------|------------|------|
| 1.252233223 | 0.923 | Check JSON below  |

```json

{
    "predicted_value": 1.252233223,
    "confidence": 0.923,
    "prediction_quality": "very confident",
    "confidence_interval": [1.025658879956537, 1.9702775375019028],
    "important_missing_information": [],
    "confidence_composition": {},
    "extra_insights": {
        "if_missing": [{
            "income": 0.6966906986877563
        }, {
            "production": 2.5382917051924445
        }, {
            "savings": 1.169812868271305
        }, {
            "unemployment": 1.3
            443338862946717
        }]
    }
}
```
### Delete the model

To delete the predictor that you have previously created, you need to delete it from `mindsdb.predictors` table. The name should be equal to name added in the INSERT statement while creating the predictor, e.g:

```sql
DELETE FROM mindsdb.predictors WHERE name='us_consumption'
```

### Train and predict multiple features

You can train a model that will predict multiple features by adding a comma separated features values in the predict column. e.g to predict the `consumption` and a `income`:

```sql
INSERT INTO
   mindsdb.predictors(name, predict, select_data_query, training_options) 
VALUES
   ('us_consumption', 'consumption, income', 'SELECT * FROM us_consumption', "option,value"});
```
And query it using the `select_data_query`:

```sql
SELECT
   consumption AS predicted,
FROM
   mindsdb.us_consumption 
WHERE
    select_data_query='SELECT income FROM us_consumption';
```

The requirements to query with `select_data_query` are:

* It must be a valid SQL statement
* It must return columns with names the same as predictor fields.


To get additional information follow the AiTables in [MySQL tutorial](https://docs.mindsdb.com/databases/tutorials/AiTablesInMySQL/).

## Extending default configuration

Before using SQL clients to connect MindsDB and databases, you will need to create additional configuration before starting MindsDB Server. Create a new `config.json` file and send the file location as parameteter to `--config` before starting the server. Check the examples below to preview the configuration required for each database.

!!! Info "Required vs Optional"
    - [X] means required config key
    - [ ] means optional config key

### PostgreSQL configuration

<details class="success">
   <summary> config.json example (click to expand)</summary> 
```json
{
   "api": {
       "http": {
           "host": "127.0.0.1",
           "port": "47334"
       },
       "mysql": {
           "host": "127.0.0.1",
           "password": "pass",
           "port": "47335",
           "user": "root"
       }
   },
   "config_version": "1.4",
   "debug": true,
   "integrations": {
       "default_postgres": {
           "database": "postgres",
           "publish": true,
           "host": "localhost",
           "password": "postgres",
           "port": 5432,
           "type": "postgres",
           "user": "postgres"
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

All of the options that should be added to the `config.json` file are:

* [x] api['http] -- This key is used for starting the MindsDB http server by providing:
    * host(default 127.0.0.1) - The mindsDB server address.
    * port(default 47334) - The mindsDB server port.
* [x] api['mysql'] -- This key is used for database integrations that works through MySQL protocol. The required keys are:
    * user(default root).
    * password(default empty).
    * host(default 127.0.0.1).
    * port(default 47335).
* [x] integrations['default_postgres'] -- This key specifies the integration type, in this case `default_postgres`. The required keys are:
    * user(default postgres) - The Postgres user name.
    * host(default 127.0.0.1) - Connect to the PostgreSQL server on the given host.
    * password - The password of the Postgres account.
    * type - Integration type(mariadb, postgresql, mysql, clickhouse, mongodb).
    * port(default 5432) - The TCP/IP port number to use for the connection.
    * publish(true|false) - Enable PostgreSQL integration.
* [ ] log['level'] -- The logging configuration(not required):
    * console - "INFO", "DEBUG", "ERROR".
    * file - Location of the log file.
* [x] storage_dir -- The directory where mindsDB will store models and configuration.

### MySQL configuration

<details class="success">
   <summary> config.json example (click to expand)</summary> 
```json
{
   "api": {
       "http": {
           "host": "127.0.0.1",
           "port": "47334"
       },
       "mysql": {
           "host": "127.0.0.1",
           "password": "pass",
           "port": "47335",
           "user": "root"
       }
   },
   "config_version": "1.4",
   "debug": true,
   "integrations": {
      "default_mysql": {
           "publish": true,
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

All of the available settings that should be added to the `config.json` file are:

* [x] api['http'] -- This key is used for starting the MindsDB HTTP server by providing:
    * host(default 127.0.0.1) - The mindsDB server address.
    * port(default 47334) - The mindsDB server port.
* [x] api['mysql'] -- This key is used for database integrations that work through MySQL protocol. The required keys are:
    * user(default root).
    * password(default empty).
    * host(default 127.0.0.1).
    * port(default 47335).
* [x] integrations['default_mysql'] -- This key specifies the integration type -- in this case `default_mysql`. The required keys are:
    * user(default root) - The MySQL user name.
    * host(default 127.0.0.1, don't use localhost here) - Connect to the MySQL server on the given host.
    * password - The password of the MySQL account.
    * type - Integration type(mariadb, postgresql, mysql, clickhouse, mongodb).
    * port(default 3306) - The TCP/IP port number to use for the connection.
    * publish(true|false) - Enable MySQL integration.
* [ ] log['level'] -- The logging configuration(optional):
    * console - "INFO", "DEBUG", "ERROR".
    * file - Location of the log file.
* [x] storage_dir -- The directory where mindsdb will store models and configuration.


### MariaDB configuration

<details class="success">
   <summary> config.json example (click to expand)</summary> 
```json
{
   "api": {
       "http": {
           "host": "127.0.0.1",
           "port": "47334"
       },
       "mysql": {
           "host": "127.0.0.1",
           "password": "pass",
           "port": "47335",
           "user": "root"
       }
   },
   "config_version": "1.4",
   "debug": true,
   "integrations": {
       "default_mariadb": {
           "host": "localhost",
           "password": "pass",
           "port": 3306,
           "publish": true,
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

All of the options that should be added to the `config.json` file are:

* [x] api['http'] -- This key is used for starting the MindsDB HTTP server by providing:
    * host(default 127.0.0.1) - The mindsDB server address.
    * port(default 47334) - The mindsDB server port.
* [x] api['mysql'] -- This key is used for database integrations that work through MySQL protocol. The required keys are:
    * user(default root).
    * password(default empty).
    * host(default 127.0.0.1).
    * port(default 47335).
* [x] integrations['default_mariadb'] -- This key specifies the integration type, in this case `default_mariadb`. The required keys are:
    * user(default root) - The MariaDB user name.
    * host(default 127.0.0.1) - Connect to the MariaDB server on the given host.
    * password - The password of the MariaDB account.
    * type - Integration type(mariadb, postgresql, mysql, clickhouse, mongodb).
    * port(default 3306) - The TCP/IP port number to use for the connection.
    * publish(true|false) - Enable MariaDB integration.
* [ ] log['level'] -- The logging configuration(optional):
    * console - "INFO", "DEBUG", "ERROR".
    * file - Location of the log file.
* [x] storage_dir -- The directory where mindsDB will store models and configuration.


### Clickhouse configuration

<details class="success">
   <summary> config.json example (click to expand)</summary> 
```json
{
   "api": {
       "http": {
           "host": "127.0.0.1",
           "port": "47334"
       },
       "mysql": {
           "host": "127.0.0.1",
           "password": "pass",
           "port": "47335",
           "user": "root"
       }
   },
   "config_version": "1.4",
   "debug": true,
   "integrations": {
       "default_clickhouse": {
           "database": "default",
           "published": true,
           "type": "clickhouse",
           "host": "localhost",
           "password": "pass",
           "port": 8123,
           "user": "default"
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

All of the options that should be added to the `config.json` file are:


* [x] api['http] -- This key is used for starting the MindsDB http server by providing:
    * host(default 127.0.0.1) - The mindsDB server address.
    * port(default 47334) - The mindsDB server port.
* [x] api['mysql'] -- This key is used for database integrations that work through MySQL protocol. The required keys are:
    * user(default root).
    * password(default empty).
    * host(default 127.0.0.1).
    * port(default 47335).
* [x] integrations['default_clickhouse'] -- This key specifies the integration type in this case `default_clickhouse`. The required keys are:
    * user(default is default user) - The ClickHouse user name.
    * host(default 127.0.0.1) - Connect to the ClickHouse server on the given host.
    * password - The password of the ClickHouse user.
    * type - Integration type(mariadb, postgresql, mysql, clickhouse, mongodb).
    * port(default 8123) - The TCP/IP port number to use for the connection.
    * publish(true|false) - Enable ClickHouse integration.
* [ ] log['level'] -- The logging configuration(not required):
    * console - "INFO", "DEBUG", "ERROR".
    * file - Location of the log file.
* [x] storage_dir -- The directory where mindsDB will store models and configuration.

### SQL Server configuration

<details class="success">
   <summary>config.json example (click to expand)</summary> 
```json
{
   "api": {
       "http": {
           "host": "127.0.0.1",
           "port": "47334"
       },
       "mysql": {
           "host": "127.0.0.1",
           "database": "mindsdb",
           "password": "123456",
           "port": "47335",
           "ssl": true,
           "user": "root"
       }
   },
   "config_version": "1.4",
   "debug": true,
   "integrations": {
       "default_mssql": {
           "host": "localhost",
           "password": "pass",
           "port": 1433,
           "publish": true,
           "type": "mssql",
           "user": "sa"
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

All of the options that should be added to the `config.json` file are:

* [x] api['http'] -- This key is used for starting the MindsDB HTTP server by providing:
    * host(default 127.0.0.1) - The mindsDB server address.
    * port(default 47334) - The mindsDB server port.
* [x] api['mysql'] -- This key is used for database integrations that work through MySQL protocol. The required keys are:
    * user(default root).
    * password(default 123456) - Required to have a password, since Microsoft SQL will use default user pass. This is the password for MindsDB MySQL API.
    * database - The name of the server that mindsdb will start.
    * ssl(default true) -- Use SSL true/false.
    * host(default 127.0.0.1).
    * port(default 47335).
* [x] integrations['default_mssql'] -- This key specifies the integration type, in this case `default_mssql`. The required keys are:
    * user(default sa) - The Microsoft SQL Server user name.
    * host(default 127.0.0.1) - Connect to the Microsoft SQL Server on the given host.
    * password - The password of the Microsoft SQL Server user.
    * type - Integration type(mariadb, postgresql, mysql, clickhouse, mongodb).
    * port(default 1433) - The TCP/IP port number to use for the connection.
    * publish(true|false) - Enable Microsoft SQL Server integration.
* [ ] log['level'] -- The logging configuration(optional):
    * console - "INFO", "DEBUG", "ERROR".
    * file - Location of the log file.
* [x] storage_dir -- The directory where mindsDB will store models and configuration.


### MongoDB Configuration


<details class="success">
   <summary> config.json example (click to expand)</summary> 
```json
{
    "api": {
        "http": {
            "host": "127.0.0.1",
            "port": "47334"
        },
        "mysql": {}
        "mongodb": {
            "host": "127.0.0.1",
            "port": "47336"
        }
    },
    "config_version": "1.4",
    "debug": true,
    "integrations": {},
    "storage_dir": "/mindsdb_storage"
}
```       
</details>

All of the options that should be added to the `config.json` file are:

* [x] api['http'] -- This key is used for starting the MindsDB HTTP API by providing:
    * host(default 127.0.0.1) - MindsDB server address.
    * port(default 47334) - MindsDB server port.
* [x] api['mongodb'] -- This key is used for starting MindsDB Mongo API by providing:
    * host(default 127.0.0.1) - MindsDB Mongo API address.
    * port(default 47335) - MindsDB Mongo API port.
* [ ] api['mysql] -- This key is used for starting MindsDB MySQL API. Leave it empty if you work only with MongoDB.
* [ ] config_version(latest 1.4) - The version of config.json file. 
* [ ] debug(true|false)
* [ ] integrations[''] -- This key specifies the integration options with other SQL databases. Leave it empty if you work only with MongoDB. 
* [ ] log['level'] -- The logging configuration(optional):
    * console - "INFO", "DEBUG", "ERROR".
    * file - Location of the log file.
* [x] storage_dir -- The directory where mindsDB will store models and configuration files.
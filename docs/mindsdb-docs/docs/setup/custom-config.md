# Custom Configuration of MindsDB Locally

To follow this guide, please make sure you have a local installation of MindsDB. [Here](https://docs.mindsdb.com/setup/self-hosted/pip/source/), you can find out how to install MindsDB locally.

## Starting MindsDB with Default Configuration

It is very straightforward to start MindsDB locally with the default config file - just run the commands below.

```bash
source mindsdb/bin/activate
```

```bash
python -m mindsdb
```

Now you can access your MindsDB locally at `127.0.0.1:47334`.

## Starting MindsDB with Custom Configuration

First, you should prepare a `config.json` file based on [the config file from the MindsDB GitHub repository](https://github.com/mindsdb/mindsdb/blob/staging/mindsdb/utilities/config.py#L35,L72).

Or you can use the template below and substitute your custom configuration values.

```bash
{
    "permanent_storage": {
        "location": "local"
    },
    "paths": {},
    "log": {
        "level": {
            "console": "INFO",
            "file": "DEBUG",
            "db": "WARNING"
        }
    },
    "debug": false, 
    "integrations": {},
    "api": {
        "http": {
            "host": "127.0.0.1",
            "port": "47334"
        },
        "mysql": {
            "host": "127.0.0.1",
            "password": "",
            "port": "47335",
            "user": "mindsdb",
            "database": "mindsdb",
            "ssl": true
        },
        "mongodb": {
            "host": "127.0.0.1",
            "port": "47336",
            "database": "mindsdb"
        }
    },
    "cache": {
        "type": "local"
    },
    "force_dataset_removing": false
}
```

Now that your `config.json` file is ready, run the command below to start MindsDB locally with your custom configuration.

```bash
python -m mindsdb --config=config-file.json
```

You can access your MindsDB locally at `127.0.0.1:47334`, or any other IP address and port combination if you altered them.

!!! tip "What's next?"
    We recommend you to follow one of our tutorials or learn more about the [MindsDB Database](/sql/table-structure/).

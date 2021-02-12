# TIMESERIES PREDICTION LATENCY BENCHMARK


## Structure

 - `config.yml`:
    - `database` - db connection info (required)
    - `datasets` - datasets info:
        - `target` - prediction column (required)
        - `handler_file` - .py file with initial data hanlder. Some data may require it, to add/remove some data. handler is a function with next signature `handler(df) where df is a pandas.DataFrame` (optional)

- `schemal.py` - describes dataset table structures
- `prepare.py` - all code to create/train models and to set up test environment (launch docker with db, create tables, upload data to db, launch mindsdb)
- `test.py` - general file with test code mostly

**PLEASE NOTE: current version is set up to work with metro_traffic_ts and monthly_sunspots datasets. Compatibility with other datasets wasn't tested**

## Add new dataset
- Add table schema in `schema.py`
- Create file with handler function if neccessary
- Add new dataset in `config.yml`

## Results
Benchmark prints result DataFrame at the end and also save it into .csv file

## Launch params
You may get this info by executing `python test.py --help`:

 - `datasets_path` - path to `benchmark-private/benchmark/datasets` dir (required)

 - `--config_path` - path to config file. If not specified, related path from  test_dir will be used, assuming that it is a standard repository structure.

 - `--no_docker` - In this case local DB will be used. Assuming that it is already installed on the host system. If not specified, docker container is used.

 - `--skip_datasource` - skip datasource preparation. May be provided if there are prepared test/train datasources from previous launch.

 - `--skip_db` - do not upload test data to database. Make sence only if local DB, installed on host is used

 - `--skip_train_models` - do not train model. Make sence if models already trained in previous launches.

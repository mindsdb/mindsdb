To run test, execute from project root:
```
python3 /tests/integration_tests/flows/test_{name}.py
```
Integration tests can be run in two modes, defined by env variable USE_EXTERNAL_DB_SERVER:
1. using local databases. For that:  
1.1 set env `USE_EXTERNAL_DB_SERVER=0`  
1.2 define database credentials in `tests/integration_tests/flows/config/config.json`
1.3 if database does not contain test table, define env `DATASETS_PATH=path/to/test_data`
1.4 run database and test
2. using remote databases. For that:  
2.1 set env `USE_EXTERNAL_DB_SERVER=1`  
2.2 save database credentials file in home dir: `.mindsdb_credentials.json`  
2.3 save db machine key in `~/.ssh/db_machine`  
2.4 run test
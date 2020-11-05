To run test, execute from project root:
```
python3 /tests/integration_tests/flows/test_{name}.py
```
Integration tests can be run in two modes, defined by env variable USE_EXTERNAL_DB_SERVER:
1. if USE_EXTERNAL_DB_SERVER="0", than on run test will be runned docker container with target databasde.
2. if USE_EXTERNAL_DB_SERVER="1", then ssh-tunnels to test database server will be up. For that should exists:  
* `.mindsdb_credentials.json` file in home dir.
* `~/.ssh/db_machine` with key for db machine.

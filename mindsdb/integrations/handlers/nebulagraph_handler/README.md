## Connect and Query NebulaGraph

> SQL API

```SQL
CREATE DATABASE nebula
WITH ENGINE = 'nebulagraph',
PARAMETERS = {
    "user": "root",
    "password": "nebula",
    "host": "127.0.0.1",
    "port": 9669,
    "graph_space": "basketballplayer"
};

nebula.query('SELECT * FROM nebula ("MATCH (n:player) RETURN n LIMIT 10")');
```

> Python API

```python
# pip install mindsdb_sdk

import mindsdb_sdk

# connects to the default port (47334) on localhost
server = mindsdb_sdk.connect()
# connects to the specified host and port
server = mindsdb_sdk.connect('http://127.0.0.1:47334')

nebulagraph = server.create_database(
    engine='nebulagraph',
    name='nebula',
    connection_args = {
        "user": "root",
        "password": "nebula",
        "host": "127.0.0.1",
        "port": "9669",
        "graph_space": "basketballplayer"
    }
)
```

## Test NebulaGraph Handler

```bash
cd mindsdb/integrations/handlers/nebulagraph_handler
pytest -s tests/test_nebulagraph_handler.py
```

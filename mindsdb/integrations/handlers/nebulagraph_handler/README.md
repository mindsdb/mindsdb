## Connect and Query NebulaGraph

```SQL
CREATE DATABASE nebula
WITH ENGINE = 'nebulagraph',
PARAMETERS = {
    "user": "root",
    "password": "nebula",
    "host": "127.0.0.1",
    "port": "9669",
    "graph_space": "basketballplayer"
};

nebula.query('SELECT * FROM nebula ("MATCH (n:player) RETURN n LIMIT 10");');
```

## Test NebulaGraph Handler

```bash
cd mindsdb/integrations/handlers/nebulagraph_handler
pytest -s tests/test_nebulagraph_handler.py
```

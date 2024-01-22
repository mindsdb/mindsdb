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

nebula.query("MATCH (n:player) RETURN n LIMIT 10")
```

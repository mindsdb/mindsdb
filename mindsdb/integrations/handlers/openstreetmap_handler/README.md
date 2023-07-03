# OpenStreetMap Handler

OpenStreetMap handler for MindsDB provides interfaces to connect to OpenStreetMap via APIs and pull map data into MindsDB.

---

## Table of Contents

- [OpenStreetMap Handler](#openstreetmap-handler)
  - [Table of Contents](#table-of-contents)
  - [About OpenStreetMap](#about-openstreetmap)
  - [OpenStreetMap Handler Implementation](#openstreetmap-handler-implementation)
  - [OpenStreetMap Handler Initialization](#openstreetmap-handler-initialization)
  - [Implemented Features](#implemented-features)
  - [TODO](#todo)
  - [Example Usage](#example-usage)
    
---

## About OpenStreetMap

OpenStreetMap is a map of the world, created by people like you and free to use under an open license.
<br>
https://www.openstreetmap.org/about

## OpenStreetMap Handler Implementation

This handler was implemented using [OSMPythonTools](https://wiki.openstreetmap.org/wiki/Overpass_API), the Overpass API wrapper for Python.

## OpenStreetMap Handler Initialization

The OpenStreetMap handler is initialized with the following parameters:

- `area`: a required area to query for map data.
- `timeout`: a required timeout to use for the query.

## Implemented Features

- [x] OpenStreetMap Nodes Table for a given Area
  - [x] Support SELECT
    - [x] Support LIMIT
    - [x] Support WHERE
    - [x] Support ORDER BY
    - [x] Support column selection
- [x] OpenStreetMap Ways Table for a given Area
    - [x] Support SELECT
        - [x] Support LIMIT
        - [x] Support WHERE
        - [x] Support ORDER BY
        - [x] Support column selection
- [x] OpenStreetMap Relations Table for a given Area
    - [x] Support SELECT
        - [x] Support LIMIT
        - [x] Support WHERE
        - [x] Support ORDER BY
        - [x] Support column selection

## TODO

- [ ] Support for more OpenStreetMap tables
- [ ] Support for more OpenStreetMap queries

## Example Usage

~~~~sql
CREATE DATABASE openstreetmap_datasource
WITH
engine='openstreetmap',
parameters={
    "area": "New York City",
    "timeout": 1000
};
~~~~

```sql 
SELECT * FROM nodes LIMIT 10;
```

```sql
SELECT * FROM nodes WHERE id = 1;
```

```sql
SELECT * FROM nodes ORDER BY id DESC LIMIT 10;
```

```sql
SELECT id, latitude, longitude FROM nodes LIMIT 10;
```

```sql
SELECT * FROM ways LIMIT 10;
```

```sql
SELECT * FROM ways WHERE id = 1;
```

```sql
SELECT * FROM ways ORDER BY id DESC LIMIT 10;
```

```sql
SELECT id, nodes, tags FROM ways LIMIT 10;
```

```sql
SELECT * FROM relations LIMIT 10;
```

```sql
SELECT * FROM relations WHERE id = 1;
```

```sql
SELECT * FROM relations ORDER BY id DESC LIMIT 10;
```

```sql
SELECT id, members, tags FROM relations LIMIT 10;
```

## Example Usage with MindsDB

```python
from mindsdb import Predictor
from mindsdb.config import CONFIG

CONFIG['USE_ASYNC'] = False

# We tell mindsDB what we want to learn and from what data
Predictor(name='openstreetmap', backend='lightwood').learn(
    from_data="https://raw.githubusercontent.com/mindsdb/main/assets/examples/openstreetmap.csv",
    to_predict='highway_type'
)

# use the model to make predictions
result = Predictor(name='openstreetmap').predict(when={'id': 1})

# you can now print the results
print(result[0])
```

## Example Output

```json
{
    "highway_type": "residential",
    "highway_type_confidence": 0.9999999999999999
}
```

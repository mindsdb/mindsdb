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

This handler was implemented using [python_overpy](https://github.com/DinoTools/python-overpy), the Overpass API wrapper for Python.

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
- [ ] Support INSERT, UPDATE and DELETE for Nodes, Ways and Node table
- [ ] Support INSERT, UPDATE and DELETE for Ways table
- [ ] Support INSERT, UPDATE and DELETE for Relations table

## Example Usage

~~~~sql
CREATE DATABASE openstreetmap_datasource
WITH
ENGINE='openstreetmap',
parameters={};
~~~~

Use the established connection to query your database:

~~~~

```sql 
SELECT * FROM openstreetmap_datasource.nodes WHERE id = 1;
```

```sql
SELECT * FROM openstreetmap_datasource.nodes WHERE area = 'your_area_value';
```

```sql
SELECT * FROM openstreetmap_datasource.nodes ORDER BY id DESC LIMIT 10;
```

```sql
SELECT id, latitude, longitude FROM openstreetmap_datasource.nodes LIMIT 10;
```

```sql
SELECT * FROM openstreetmap_datasource.ways LIMIT 10;
```

```sql
SELECT * FROM openstreetmap_datasource.ways WHERE id = 1;
```

```sql
SELECT * FROM openstreetmap_datasource.ways ORDER BY id DESC LIMIT 10;
```

```sql
SELECT id, nodes, tags FROM openstreetmap_datasource.ways LIMIT 10;
```

```sql
SELECT * FROM openstreetmap_datasource.relations LIMIT 10;
```

```sql
SELECT * FROM openstreetmap_datasource.relations WHERE id = 1;
```

```sql
SELECT * FROM openstreetmap_datasource.relations ORDER BY id DESC LIMIT 10;
```

```sql
SELECT id, members, tags FROM openstreetmap_datasource.relations LIMIT 10;
```
    
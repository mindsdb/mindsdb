# PyPI Handler
This handler allows you to interact with the data that [pypi.org](https://pypi.org) is storing on Google's BigQuery about the rates of Python packages.

## About PyPI
Python Package Index (PyPI) is a host for maintaining and storing Python packages. It's a good place for publishing your Python packages in different versions and releases.

## PyPI Handler Implementation
This implementation is based on the [`pypistats`](https://github.com/hugovk/pypistats) package.

## PyPI Handler Initialization
There is nothing needed to be passed in the database initialization process. You can create the database via the following flow.

```sql
CREATE DATABASE pypi_datasource
WITH ENGINE = 'pypi';
```

Once you execute this query, you'll have access to all the following tables.

- Overal Table: `pypi_datasource.overall`
- Recent Table: `pypi_datasource.recent`
- Python Major Table: `pypi_datasource.python_major`
- Python Minor Table: `pypi_datasource.python_minor`
- System Table: `pypi_datasource.system`

## Example Usage
Each table has its `WHERE` clause(s) and condition(s) as follows.

- `overall`
  - `include_mirrors`: `{true, false}`
- `python_major`
  - `version`: `{2, 3, ...}`
- `python_minor`
  - `version`: `{2.7, 3.2, ...}`
- `system`:
  - `os`: `{"Windows", "Linux", "Darwin", ...}`

### All the recent downloads
```sql
SELECT *
FROM pypi_datasource.recent WHERE package="mindsdb";
```
```sql
SELECT last_day, last_month
FROM pypi_datasource.recent WHERE package="mindsdb";
```

### Overall downloads with mirrors included
```sql
SELECT *
FROM pypi_datasource.overall WHERE package="mindsdb" AND include_mirrors=true;
```

### Overall downloads on Python2.7
```sql
SELECT *
FROM pypi_datasource.python_minor WHERE package="mindsdb" AND version=2.7;
```

### All the downloads on the Linux-based distros
```sql
SELECT downloads
FROM pypi_datasource.system WHERE package="mindsdb" AND os="Linux";
```

## Implemented Features
- [x] Database initialization
- [x] Tracking the downloads rate
  - [x] Overall downloads
  - [x] Recent downloads
- [x] System-based filtering
- [x] Python version-based filtering

## TODO
- [ ] Tracking the dependency graph
- [ ] Packages' metadata filtering
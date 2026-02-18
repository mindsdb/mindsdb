# PyPI Handler
This handler allows you to interact with the data that [pypi.org](https://pypi.org) is storing on Google's BigQuery about the rates of Python packages.

## About PyPI
Python Package Index (PyPI) is a host for maintaining and storing Python packages. It's a good place for publishing your Python packages in different versions and releases.

## PyPI Handler Implementation
This implementation is based on the RESTful service that [pypistats.org](https://pypistats.org) is serving.

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

- `recent`
  - `period`: `{day, week, month}`
- `overall`
  - `mirrors`: `{true, false}`
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
SELECT *
FROM pypi_datasource.recent WHERE package="mindsdb" AND period="day";
```

### Overall downloads (only mirrors included)
```sql
SELECT *
FROM pypi_datasource.overall WHERE package="mindsdb" AND mirrors=true;
```

### Overall downloads on CPython==2.7
```sql
SELECT *
FROM pypi_datasource.python_minor WHERE package="mindsdb" AND version="2.7";
```

### All the downloads on the Linux-based distros
```sql
SELECT date, downloads
FROM pypi_datasource.system WHERE package="mindsdb" AND os="Linux";
```

### Keep in mind..
- Each table takes a *REQUIRED* `WHERE` parameter and that's nothing but the package name that is specified with the `package` keyword.
- All the `Null` recordes are ignored from viewing.
- `SELECT` query is limited by 20 records by default. You can change it to whatever amount of records you need.


## Implemented Features
- [x] Database initialization
- [x] Tracking the downloads rate
  - [x] Overall downloads
  - [x] Recent downloads
- [x] System-based filtering
- [x] Version-based filtering
  - [x] Minor-based filtering
  - [x] Major-based filtering

## TODO
- [ ] Writing tests
- [ ] Tracking the dependency graph
- [ ] Packages' metadata filtering
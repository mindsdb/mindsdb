[<Back to Table of Contents](../README.md)

## Data Sources

MindsDB uses pandas as its datasource driver, so anything you can convert into a dataframe MindsDB can work with.

There is are special implementations of datasources that already do cleaning and various tasks:

You can learn about them in [here](https://github.com/mindsdb/mindsdb/tree/master/mindsdb/libs/data_sources):

#### FileDS

```python
from mindsdb import FileDS

ds = FileDS(file, clean_header = True, clean_rows = True, custom_parser = None)

```

* ***file*** can be a path in the localsystem, stringio object or a url to a file, supported types are (csv, json, xlsx, xls).
* ***clean_header*** (default=True) clean column names, so that they don't have special characters or white spaces.
* ***clean_rows*** (default=True) Goes row by row making sure that nulls are nulls and that no corrupt data exists in them, it also cleans empty rows
* ***custom_parser*** (default=None) special function to extract the actual table structure from the file in case you need special transformations.

#### OPTIONAL

***SQLite***: Although by default python3 comes with SQLite, you would want to have SQLite versions >=(3.25.0) as it supports WINDOW functions, which can be very handy for data preparation

##### On MacOS

```bash
brew upgrade sqlite
```

You can check your version in Python:

```python
>>> import sqlite3
>>> sqlite3.version
```

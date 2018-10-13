[<Back to Table of Contents](../README.md)
# Installing MindsDB

You can install MindsDB as follows

```bash
 pip3 install mindsdb --user
```

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


##### On Windows


Install Python

https://docs.python.org/3/using/windows.html

install pip

https://pip.pypa.io/en/stable/installing/



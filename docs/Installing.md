[<Back to Table of Contents](README.md)
# Installing MindsDB

You can install MindsDB as follows

```bash
 pip3 install mindsdb --user
```


#### You need:

****MongoDB server****:  By default, MindsDB will look for it in mongodb://127.0.0.1/mindsdb

On MacOS

```bash
brew install mongodb

```

To run it

```bash
sudo mongod
```

#### Good to have

***SQLite***: Although by default python3 comes with SQLite, you would want to have SQLite versions >=(3.25.0) as it support WINDOW functions, which can be very handy for data preparation

On MacOS

```bash
brew upgrade sqlite
```

You can check your version in Python:

```python
>>> import sqlite3
>>> sqlite3.version
```
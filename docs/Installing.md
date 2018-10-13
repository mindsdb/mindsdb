[<Back to Table of Contents](../README.md)
# Installing MindsDB

You can install MindsDB 

##### On Mac or Linux 

```bash
 pip3 install mindsdb --user
```

##### On Windows 10


Follow [these instructions](https://conda.io/miniconda.html) to install Python miniConda and then from command line

```bash
conda install -c blaze sqlite3 pytorch
pip3 install mindsdb
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



[<Back to Table of Contents](../README.md)
# Installing MindsDB

You can install MindsDB 

##### On Mac or Linux 

```bash
 pip3 install mindsdb --user
```

##### On Windows 10


Follow [these instructions](https://conda.io/miniconda.html) to install Python miniConda.
 
 and then run the **anaconda prompt**: 

```bash

conda install -c blaze sqlite3 peterjc123 pytorch
pip install torchvision mindsdb
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



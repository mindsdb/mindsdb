---
id: config
title: Configuration Settings
---

MindsDB has config variables that can be set by as environment variables or on a script.

Here are some of the variables of general interest:

* `MINDSDB_STORAGE_PATH` Where MindsDB stores its data, by default it is the path where pip installs packages + `mindsdb/mindsdb_storage`
* `DEFAULT_MARGIN_OF_ERROR` The reverse of how much of the data you feed in mindsdb will sample in order to generate insights about the data. For example, if this is set to 0.4, mindsdb will sample 0.6 of the data.
* `DEFAULT_LOG_LEVEL` What logs mindsdb will display (By default this is set to: `CONST.INFO_LOG_LEVEL`)

## How to set config variables?

You can either set it as environment variables or you can do it in your script.

```python
import os

os.environ['<varname>'] = <value>

```
For example, if you want to specify a different storage directory:

```python
import os

os.environ['MINDSDB_STORAGE_PATH'] = '/home/my_wonderful_username/place_where_i_store_big_files/'

# now we import mindsdb
from mindsdb import MindsDB

```

Alternatively, we can se the variable after importing mindsdb:

```python
mindsdb.CONFIG.MINDSDB_STORAGE_PATH = '/home/my_wonderful_username/place_where_i_store_big_files/'
```


You can see all the config variables available [here](https://github.com/mindsdb/mindsdb/blob/master/mindsdb/config/__init__.py)

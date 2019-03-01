[<Back to Table of Contents](../README.md)
### CONFIG Settings

MindsDB has config variables that can be set by as environment variables or on script.

Here are some of the variables of general interest:

* `MINDSDB_STORAGE_PATH` Where MindsDB stores its data, by default it is on the pip mindsdb/storage directory 
* `STORE_INFO_IN_MONGODB` You can choose to store info about models in MongoDB or not By default it uses a local object store (Default False)
* `MONGO_SERVER_HOST` If `STORE_INFO_IN_MONGODB == True` then specify the host here 


#### How to set config variables?

You can either set it as environment variables or you can do it in your script.

```python
import os

os.environ['<varname>'] = <value>

```
For example: if you want to specify a different Mongo Server

```python
import os

os.environ['MONGO_SERVER_HOST'] = 'mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[database][?options]]'

# now we import mindsdb
from mindsdb import MindsDB

```


You can read the config variables available ([here](../mindsdb/config/__init__.py))

### CONFIG Settings

You can manipulate the default settings of MindsDB by setting env variables before importing the mindsdb module

For example: if you want to specify a different Mongo Server

```python
import os

os.environ['MONGO_SERVER_HOST'] = 'mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[database][?options]]'

# now we import mindsdb
from mindsdb import MindsDB

```


You can read the config variables available ([here](../mindsdb/config/__init__.py))
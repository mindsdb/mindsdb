
# EventStoreDBHandler 

### How it works?
This handler treats EventStoreDB streams as tables and every JSON Event's data key as column.
Events with nested JSON are flattened with underscore as the separator.

### Prerequisites

**EventStoreDB Configuration**

- RunProjections=All and to enable the $streams projection. This is required to allow MindsDB to get all the available tables i.e. streams 
- EnableAtomPubOverHTTP=True. The handler connects to EventStoreDB over the atom pub API.

### Limitations
- Stream names can only contain characters supported in SQL i.e. dots and dashes commonly used for stream names will not work at the moment.
- JSON data is flattened with underscore as separator. This is not configurable at the moment.

### Example Usage

```buildoutcfg
CREATE DATABASE securedb
WITH ENGINE = "eventstoredb",
PARAMETERS = {
  "user": "admin",
  "password": "changeit",
  "tls": True,
  "host":"localhost",
  "port":2113
  };
```
This will create a connection to a local secure node. Once this is successful, you can read about how to run machine learning algorithms on top of your EventStoreDB data at: [https://docs.mindsdb.com/ml-types](https://docs.mindsdb.com/ml-types)


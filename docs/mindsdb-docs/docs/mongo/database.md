# `#!sql databases.insertOne()` Method

## Description

MindsDB enables connections to your mongo instance via the `#!sql db.databases.insertOne()` syntax.

Our MindsDB Mongo API supports creating a connection by passing the credentials needed for connecting to the database.

## Syntax

```sql
db.databases.insertOne({
    'name': 'mongo_int', 
    'engine': 'mongodb',
    'connection_args': {
            "port": 27017,
            "host": "mongodb+srv://admin:@localhost",
            "database": "test_data"            
  }   
});
```

On execution, you should get:

```sql
{
	"acknowledged" : true,
	"insertedId" : ObjectId("62dff63c6cc2fa93e1d7f12c")
}
```

Where:

|                        | Description                                                      |
| ---------------------- | ---------------------------------------------------------------- |
| `[name]`               | Identifier for the datasource to be created                      |
| `[engine]=mongodb`     |  Engine to be selected. For MONGO API it is always mongodb      |
| `connection_args`      | `#!json {"key":"value"}` object with the connection parameters specific for mongo engine as port, host, database  |

## Example

Here is a concrete example on how to connect to the local MongoDB.

```sql
db.databases.insertOne({
    'name': 'mongo_local', 
    'engine': 'mongodb',
    'connection_args': {
            "port": 27017,
            "host": "mongodb+srv://admin:@localhost",
            "database": "test_data"            
  }   
});
```

On execution:

```sql
{
	"acknowledged" : true,
	"insertedId" : ObjectId("62dff63c6cc2fa93e1d7f12c")
}
```

## Listing Linked DATABASES

You can list linked databases as follows:

```sql
show dbs;
```

On execution:

```sql
+--------------------+
| Database           |
+--------------------+
| admin              |
| files              |
| information_schema |             
| mindsdb            |
| mongo_int          |
| views              |
+--------------------+
```

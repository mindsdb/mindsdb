# Building a new integration

This section will walk you through adding a new integration to MindsDB as data layers or predictive frameworks.


## Prerequisite

Make sure you have cloned and installed the latest staging version of MindsDB repository locally. 


### Structure

The code for integrations is located in the main MindsDB repository under [/integrations](https://github.com/mindsdb/mindsdb/tree/staging/mindsdb/integrations) directory.


```
integrations                           # Contains integrations source code
├─ handlers/                           # Each integration has its own handler dir
│  ├─ mysql_handler/                   # MySQL integration code
│  ├─ lightwood_handler/               # Lightwood integration code
│  ├─  .../                            # Other handlers
├─ libs/
│  ├─ base_handler.py                  # Base class that each handler inherit from
│  ├─ storage_handler.py               # Storage classes for each handler
└─ utilities                           # Handlers utilities dir
│  ├─ install.py                       # Installs all handlers dependencies
```

### New integration steps

If you are adding new data integration you need to extend the [`DatabaseHandler`](https://github.com/mindsdb/mindsdb/blob/staging/mindsdb/integrations/libs/base_handler.py#L106). In a case of adding a predictive framework
integration extend [`PredictiveHandler`](https://github.com/mindsdb/mindsdb/blob/staging/mindsdb/integrations/libs/base_handler.py#L114). 
Each integration needs 7 core methods:

1. `connect` – Setup storage and connection
2. `disconnect` – Terminate connection
3. `check_connection` – Health check
4. `native_query`  – Act upon a raw SQL-like command
5. `query` – Act upon a parsed SQL-like command
6. `get_tables` – List all accessible entities within handler
7. `get_columns` – Column info for a specific table entity
8. `join` – Call other handlers to merge data with predictions. Predictive handlers only


Bellow, you can find list of entitiles required to create database handler. As an exemple of database handler, please use `mysql_handler`.

### Step 1: Create `Handler` class:

Inherite it from `DatabaseHandler`

#### Set class property `name`:

It will be used inside MindsDB as name of handler. For example, it use as `ENGINE` in command

```sql
    CREATE DATABASE integration_name WITH ENGINE='postgres', PARAMETERS={'host': '127.0.0.1', 'user': 'root', 'password': 'password'}
```

#### Step 1.1: Implement `__init__`

Method should initialize the handler. `connection_data` - will contain `PARAMETERS` from `CREATE DATABASE` statement

```py
    def __init__(self, name: str, connection_data: Optional[dict], **kwargs)
        """ Initialize the handler
        Args:
            name (str): name of particular handler instance
            connection_data (dict): parameters for connecting to the database
            **kwargs: arbitrary keyword arguments.
        """
```

#### Step 1.2: Implement `connect`

The connect method should set up the connection as:

```py
    def connect(self) -> HandlerStatusResponse:
        """ Set up any connections required by the handler
        Should return output of check_connection() method after attempting
        connection. Should switch self.is_connected.
        Returns:
            HandlerStatusResponse
        """
```

#### Step 1.3: Implement `disconnect`

The disconnect method should close the existing connection as:

```py
  def disconnect(self):
        """ Close any existing connections
        Should switch self.is_connected.
        """
        self.is_connected = False
        return self.is_connected

```

#### Step 1.4: Implement `check_connection`

The check_connection method is used to perform the health check for the connection:

```py
def check_connection(self) -> HandlerStatusResponse:
        """ Cehck connection to the handler
        Returns:
            HandlerStatusResponse
        """
```

#### Step 1.5: Implement `native_query`

The native_query method is used to run command on native database language:

```py
def native_query(self, query: Any) -> HandlerStatusResponse:
        """Receive raw query and act upon it somehow.
        Args:
            query (Any): query in native format (str for sql databases,
                dict for mongo, etc)
        Returns:
            HandlerResponse
        """
```

#### Step 1.6: Implement `query`

The query method is used to run parsed SQL command:

```py
def query(self, query: ASTNode) -> HandlerStatusResponse:
        """Receive query as AST (abstract syntax tree) and act upon it somehow.
        Args:
            query (ASTNode): sql query represented as AST. May be any kind
                of query: SELECT, INTSERT, DELETE, etc
        Returns:
            HandlerResponse
        """
```


#### Step 1.7: Implement `get_tables`

The get_tables method is used to list tables:

```py
def get_tables(self) -> HandlerStatusResponse:
       """ Return list of entities
        Return list of entities that will be accesible as tables.
        Returns:
            HandlerResponse: shoud have same columns as information_schema.tables
                (https://dev.mysql.com/doc/refman/8.0/en/information-schema-tables-table.html)
                Column 'TABLE_NAME' is mandatory, other is optional.
        """
```

#### Step 1.8: Implement `get_columns`

The get_tables method is used to list tables:

```py
def get_columns(self, table_name: str) -> HandlerStatusResponse:
      """ Returns a list of entity columns
        Args:
            table_name (str): name of one of tables returned by self.get_tables()
        Returns:
            HandlerResponse: shoud have same columns as information_schema.columns
                (https://dev.mysql.com/doc/refman/8.0/en/information-schema-columns-table.html)
                Column 'COLUMN_NAME' is mandatory, other is optional. Hightly
                recomended to define also 'DATA_TYPE': it should be one of
                python data types (by default it str).
        """
```

### Step 2: Create `connection_args` dict:

Dict should contain possible arguments to establish connection.

### Step 3: Create `connection_args_example` dict:

Dict should example of connection arguments.


### Step 4: Export all required entities:

Module should export:
`Handler` - handler class
`version` - version of handler
`name` - name of the handler (same as Handler.name)
`type` - type of the handler (is it DATA of ML handler)
`icon_path` - path to file with database icon
`title` - short description of handler
`description` - description of handler
`connection_args` - dict with connection args
`connection_args_example` - example of connection args
`import_error` - error message, in case if is not possible to import `Handler` class

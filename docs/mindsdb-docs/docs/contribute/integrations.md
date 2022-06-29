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

Bellow, you can find examples of each methods that uses the `BaseHandler` methods as an example.

#### Step 1: Implement `connect`

The connect method should set up the connection as:

```py
    def connect(self, **kwargs) -> HandlerStatusResponse:
        """ Set up any connections required by the handler
        Should return output of check_connection() method after attempting
        connection. Should switch self.is_connected.
        Args:
            **kwargs: Arbitrary keyword arguments.
        Returns:
            HandlerStatusResponse
        """
```

#### Step 2: Implement `disconnect`

The disconnect method should close the existing connection as:

```py
  def disconnect(self):
        """ Close any existing connections
        Should switch self.is_connected.
        """
        self.is_connected = False
        return self.is_connected

```

#### Step 3: Implement `check_connection`

The check_connection method is used to perform the health check for the connection:

```py
def check_connection(self) -> HandlerStatusResponse:
        """ Cehck connection to the handler
        Returns:
            HandlerStatusResponse
        """
```

#### Step 4: Implement `native_query`

The native_query method is used to run a raw SQL command:

```py
def native_query(self) -> HandlerStatusResponse:
        """Receive raw query and act upon it somehow.
        Args:
            query (Any): query in native format (str for sql databases,
                dict for mongo, etc)
        Returns:
            HandlerResponse
        """
```

#### Step 5: Implement `query`

The query method is used to run parsed SQL command:

```py
def query(self) -> HandlerStatusResponse:
        """Receive query as AST (abstract syntax tree) and act upon it somehow.
        Args:
            query (ASTNode): sql query represented as AST. May be any kind
                of query: SELECT, INTSERT, DELETE, etc
        Returns:
            HandlerResponse
        """
```


#### Step 6: Implement `get_tables`

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

#### Step 7: Implement `get_columns`

The get_tables method is used to list tables:

```py
def get_tables(self) -> HandlerStatusResponse:
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
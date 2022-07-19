# Building a new integration

This section will walk you through adding a new integration to MindsDB as data layers or predictive frameworks.


## Prerequisite

Make sure you have cloned and installed the latest staging version of MindsDB repository locally. 

## What are handlers?

At the heart of the MindsDB philosophy lies the belief that predictive insights are best leveraged when produced as close as possible to the data layer. Usually, this "layer" is a SQL-compatible database, but it could also be a non-SQL database, data stream, or any other tool that interacts with data stored somewhere else.

The above description fits an enormous set of tools that people use across the software industry. The complexity increases further by bringing Machine Learning into the equation, as the set of popular ML tools is similarly huge. We aim to support most technology stacks, requiring a simple integration procedure so that anyone is able to easily contribute the necessary "glue" to enable any predictive system for usage within data layers.

This motivates the concept of _handlers_, which is an abstraction for the two types of entities mentioned above: data layers and predictive systems. Handlers are meant to enforce a common and sufficient set of behaviors that all MindsDB-compatible entities should support. By creating a handler, the target system is effectively integrated into the wider MindsDB ecosystem.

## Structure of a handler

Technically speaking, a handler is a self-contained Python package that will have everything required for MindsDB to interact with it, including aspects like dependencies, unit tests, and continuous integration logic. It is up to the author to determine the nature of the package (e.g. close or open source, version control, etc.), although we encourage opening pull requests to expand the default set of supported tools.

The entrypoint is a class definition that should inherit from either `integrations.libs.base_handler.DatabaseHandler` or `integrations.libs.base_handler.PredictiveHandler` depending on the type of system that is integrated. `integrations.libs.base_handler.BaseHandler` defines all the common methods that have to be overwritten in order to achieve a functional implementation.

Apart from the above, structure is not enforced and the package can be built arranged into whatever design is preferred by the author.

### Structure within the MindsDB repository

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

### Core methods

Apart from `__init__()`, there are seven core methods that the handler class has to implement, these are: `connect(), disconnect(), check_connection(), native_query(), query(), get_tables(), get_columns()`. It is recommended to check actual examples in the codebase to get an idea of what may go into each of these methods, as they can change a bit depending on the nature of the system being integrated. Respectively, their main purpose is:

1. `connect`: perform any necessary steps to connect to the underlying system
2. `disconnect`: if needed, gracefully close connections established in `connect`
3. `check_connection`: evaluate if the connection is alive and healthy. Will be frequently called.
4. `native_query`: should parse any _native_ statement string and act upon it (e.g. raw SQL-like commands).
5. `query`: takes a parsed SQL-like command (in the form of an abstract syntax tree) and executes it. An example would be a `CREATE PREDICTOR` statement for predictive handlers, which is _not_ native syntax as databases have no notion of a `PREDICTOR` entity.
6. `get_tables`: All available `tables`s should be listed and returned. Each handler should decide what a `table` will mean for the underlying system when interacting with it from the data layer. Typically, this means actual tables for data handlers, and machine learning models (or predictive handlers.
7. `get_columns`: Each table registered in the handler will have one or more columns (with their respective data types) that will be returned when calling this method.

As stated in the above section, authors can opt for adding private methods, new files, folders, or any combination of these to structure all the necessary work that will enable the methods above to work as intended.

### Predictor-specific behavior

For predictive handlers, there is an additional method that is fundamental:

* `join()`: triggers a specific model to generate predictions given some input data. This behavior manifests in the SQL API when doing any type of `JOIN` operation between tables from a predictive and data handler.

### Parsing SQL

Whenever a string that contains SQL needs to be parsed, it is heavily recommended to opt for using the `mindsdb_sql` package, which contains its own parser that fully supports the MindsDB SQL dialect and partially supports the common SQL dialect. There is also a "render" feature to map other dialects into the already supported ones.

### Storing internal state

Most handlers need to store internal metadata, ranging from a list of registered tables to implementation-specific details that will greatly vary from one case to another.

The recommendation for storing these bits of information is to opt for storage handlers (located in `integrations.libs.storage_handler`). We currently support two options, either a Sqlite or a Redis backend. In both cases, the premise is the same: a key-value store system is setup so that interfaces are kept simple and clean, exposing only `get()` and `set()` methods for usage within the data and predictive handlers.

> Note: for ML frameworks, opt for storing the path to your model weights inside the KV storage, saving weights in optimized formats preferred by the system, like `.h5`)

### Formatting output

When it comes to building the response of the public methods, the output should be wrapped by the `HandlerResponse` and `HandlerStatusResponse` classes (located in `mindsdb.integrations.libs.response`), which are used by MindsDB executioner to orchestrate and coordinate multiple handler instances in parallel.

## Other common methods

Under `mindsdb.integrations.libs.utils`, contributors can find various methods that may be useful while implementing new handlers, with a focus on predictive handlers.

## How to write a handler

We wrap up this page by going through all the above information step by step. Remember, if you are adding new data integration you need to extend the [`DatabaseHandler`](https://github.com/mindsdb/mindsdb/blob/staging/mindsdb/integrations/libs/base_handler.py#L106). In a case of adding a predictive framework integration extend [`PredictiveHandler`](https://github.com/mindsdb/mindsdb/blob/staging/mindsdb/integrations/libs/base_handler.py#L114). 
In both cases we need 7 core methods:

1. `connect` – Setup storage and connection
2. `disconnect` – Terminate connection
3. `check_connection` – Health check
4. `native_query`  – Act upon a raw SQL-like command
5. `query` – Act upon a parsed SQL-like command
6. `get_tables` – List all accessible entities within handler
7. `get_columns` – Column info for a specific table entity

And additionally for predictive handlers:
8. `join` – Call other handlers to merge data with predictions. 

Below, you can find a list of entities required to create a database handler.

### Step 1: Create `Handler` class:

Each DatabaseHandler should inherit from `DatabaseHandler` class.

#### Set class property `name`:

It will be used inside MindsDB as name of handler. For example, the name is used use an `ENGINE` in `CREATE DATABASE` statement:

```sql
    CREATE DATABASE integration_name WITH ENGINE='postgres', +
    PARAMETERS={'host': '127.0.0.1', 'user': 'root', 'password': 'password'}
```

#### Step 1.1: Implement `__init__`

This method should initialize the handler. the `connection_data` argument will contain `PARAMETERS` from `CREATE DATABASE` statement as `user`, `password` etc.

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

The `connection_arg` dictinonary should contain all required arguments to establish the connection.

### Step 3: Create `connection_args_example` dict:

The `connection_args_example` dictinonary should contain an example of all required arguments to establish the connection.


### Step 4: Export all required variables:

In `__init__` file export:

* `Handler` - handler class
* `version` - version of handler
* `name` - name of the handler (same as Handler.name)
* `type` - type of the handler (is it DATA of ML handler)
* `icon_path` - path to file with database icon
* `title` - short description of handler
* `description` - description of handler
* `connection_args` - dict with connection args
* `connection_args_example` - example of connection args
* `import_error` - error message, in case if is not possible to import `Handler` class

E.g:

```py
title = 'Trino'
version = 0.1
description = 'Integration for connection to TrinoDB'
name = 'trino'
type = HANDLER_TYPE.DATA
icon_path = 'icon.png'

__all__ = [
    'Handler', 'version', 'name', 'type', 'title',
    'description', 'connection_args_example'
    'Handler', 'version', 'name', 'type', 'title', 'description', 'icon_path'
]
```

For real examples, we encourage you to inspect the following handlers inside the MindsDB repository:
* MySQL
* Postgres
* MLflow
* Lightwood
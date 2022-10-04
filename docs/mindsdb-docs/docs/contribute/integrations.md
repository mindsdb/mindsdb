# Building a New Integration

In this section, you'll find how to add new integrations to MindsDB, either as *data layers* or *predictive frameworks*.

## Prerequisite

You should have the latest staging version of the MindsDB repository installed locally. Follow [this guide](/contribute/install/) to learn how to install MindsDB for development.

## What are Handlers?

At the heart of the MindsDB philosophy lies the belief that predictive insights are best leveraged when produced as close as possible to the data layer. Usually, this *data layer* is a SQL-compatible database, but it could also be a non-SQL database, data stream, or any other tool that interacts with data stored somewhere else.

The above description fits an enormous set of tools used across the software industry. The complexity increases further by bringing Machine Learning into the equation, as the set of popular ML tools is similarly huge. We aim to support most technology stacks, requiring a simple integration procedure so that anyone can easily contribute the necessary *glue* to enable any predictive system for usage within data layers.

This motivates the concept of *handlers*, which is an abstraction for the two types of entities mentioned above: *data layers* and *predictive frameworks*. Handlers are meant to enforce a common and sufficient set of behaviors that all MindsDB-compatible entities should support. By creating a handler, the target system is effectively integrated into the wider MindsDB ecosystem.

## Handlers in the MindsDB Repository

The codes for integrations are located in the main MindsDB repository under the [/integrations](https://github.com/mindsdb/mindsdb/tree/staging/mindsdb/integrations) directory.

```
integrations                      # Contains integrations source codes
├─ handlers/                           # Each integration has its own handler directory
│  ├─ mysql_handler/                       # MySQL integration code
│  ├─ lightwood_handler/                   # Lightwood integration code
│  ├─  .../                                # Other handlers
├─ libs/                               # Handler libraries directory
│  ├─ base_handler.py                      # Each handler class inherits from this base class
│  ├─ storage_handler.py                   # Storage classes used by handlers
└─ utilities                           # Handler utility directory
│  ├─ install.py                           # Script that installs all handler dependencies
```

## Structure of a Handler

In technical terms, a handler is a self-contained Python package having everything required for MindsDB to interact with it. It includes aspects like dependencies, unit tests, and continuous integration logic. It is up to the author to determine the nature of the package, for example, closed or open source, version control, and more. Although, we encourage opening pull requests to expand the default set of supported tools.

The entry point is a class definition that should inherit either from the `integrations.libs.base_handler.DatabaseHandler` class or the `integrations.libs.base_handler.PredictiveHandler` class, depending on the type of the handler. The `integrations.libs.base_handler.BaseHandler` class defines all the methods that must be overwritten in order to achieve a functional implementation.

!!! note "Handler's Structure"
    The handler's structure is not enforced, and the package design is up to the author.

### Core Methods

Apart from the `__init__()` method, there are seven core methods that the `Handler` class has to implement, these are: `connect()`, `disconnect()`, `check_connection()`, `native_query()`, `query()`, `get_tables()`, `get_columns()`. We recommend checking actual examples in the codebase to get an idea of what goes into each of these methods, as they can change a bit depending on the nature of the system being integrated.

Let's review the purpose of each method.

| Method                 | Purpose                                                                                                                                                                                                                                                      |
|------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `connect()`            | It performs the necessary steps to connect to the underlying system.                                                                                                                                                                                         |
| `disconnect()`         | It gracefully closes connections established in the `connect()` method.                                                                                                                                                                                      |
| `check_connection()`   | It evaluates if the connection is alive and healthy. This method is called frequently.                                                                                                                                                                       |
| `native_query()`       | It parses any *native* statement string and acts upon it (for example, raw SQL commands).                                                                                                                                                                    |
| `query()`              | It takes a parsed SQL command in the form of an abstract syntax tree and executes it. A good example is the `CREATE PREDICTOR` statement for predictive handlers, which is a *non-native* syntax as databases have no notion of a `PREDICTOR` entity.        |
| `get_tables()`         | It lists and returns all the available tables. Each handler decides what a `table` means for the underlying system when interacting with it from the data layer. Typically, these are actual tables for data handlers and predictive handlers.               |
| `get_columns()`        | It returns columns of a table registered in the handler with the respective data type.                                                                                                                                                                       |

Authors can opt for adding private methods, new files and folders, or any combination of these to structure all the necessary work that will enable the core methods to work as intended.

!!! tip "Other Common Methods"
    Under the `mindsdb.integrations.libs.utils` library, contributors can find various methods that may be useful while implementing new handlers, especially the predictive handlers.

### Predictor-Specific Methods

For predictive handlers, there is an additional method that must be implemented, the `join()` method.

| Method               | Purpose                                                                                                                                                                                                                                                      |
|----------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `join()`             | It triggers a specific model to generate predictions when given some input data. This behavior manifests in the SQL API when performing any type of `JOIN` operations between tables from a predictive handler and tables from a data handler.               |

### Parsing SQL

Whenever you want to parse a string that contains SQL, we strongly recommend using the `mindsdb_sql` package. It provides a parser that fully supports the MindsDB SQL dialect and partially the standard SQL dialect. There is also a *render* feature to map other dialects into the already supported ones.

### Storing Internal State

Most handlers need to store internal metadata ranging from a list of registered tables to implementation-specific details that greatly vary from one case to another.

The recommendation for storing these bits of information is to use storage handlers from the `integrations.libs.storage_handler` library. We currently support two options: an SQLite backend and a Redis backend. In both cases, the premise is the same: a key-value store system is set up such that interfaces are simple and clean, exposing only the `get()` and `set()` methods for usage within the data and predictive handlers.

!!! note "ML Frameworks"
    In the case of ML frameworks, opt for storing the path to your model weights inside the KV storage and saving weights in optimized formats preferred by the system, like `.h5`.

### Formatting Output

When it comes to building the response of the public methods, the output should be wrapped by the `HandlerResponse` or `HandlerStatusResponse` class located in the `mindsdb.integrations.libs.response` library. These classes are used by the MindsDB executioner to orchestrate and coordinate multiple handler instances in parallel.

## How to Write a Handler

Let's go through all the above information step by step.

!!! info "Extending the Classes"
    Extend the [`DatabaseHandler`](https://github.com/mindsdb/mindsdb/blob/staging/mindsdb/integrations/libs/base_handler.py#L106) class when adding a new data handler.<br/>
    Extend the [`PredictiveHandler`](https://github.com/mindsdb/mindsdb/blob/staging/mindsdb/integrations/libs/base_handler.py#L114) class when adding a new predictive handler.

The seven core methods must be implemented, both for a data handler and a predictive handler.

1. The `connect()` method sets up storage and connection.
2. The `disconnect()` method terminates the connection.
3. The `check_connection()` method validates the connection.
4. The `native_query()` method acts upon a raw SQL command.
5. The `query()` method acts upon a parsed SQL command.
6. The `get_tables()` method lists all accessible entities within the handler.
7. The `get_columns()` method returns columns for a specific table entity.

And additionally, for predictive handlers:

1. The `join()` method calls other handlers to merge data with predictions. 

## Creating a Database Handler

Below is the list of entities required to create a database handler.

### Creating the `Handler` Class

Each database handler should inherit from the [`DatabaseHandler`](https://github.com/mindsdb/mindsdb/blob/staging/mindsdb/integrations/libs/base_handler.py#L106) class.

* Setting the `name` class property:

    MindsDB uses it internally as a name of the handler.

    For example, the `CREATE DATABASE` statement uses the handler's name.

    ```sql
    CREATE DATABASE integration_name
    WITH ENGINE='postgres',             --- here, the handler's name is `postgres`
    PARAMETERS={
    'host': '127.0.0.1',
    'user': 'root',
    'password': 'password'
    };
    ```

* Implementing the `__init__()` method:

    This method initializes the handler. The `connection_data` argument contains the `PARAMETERS` from the `CREATE DATABASE` statement, such as `user`, `password`, etc.

    ```py
    def __init__(self, name: str, connection_data: Optional[dict], **kwargs)
        """ Initialize the handler
        Args:
            name (str): name of particular handler instance
            connection_data (dict): parameters for connecting to the database
            **kwargs: arbitrary keyword arguments.
        """
    ```

* Implementing the `connect()` method:

    The `connect()` method sets up the connection.

    ```py
    def connect(self) -> HandlerStatusResponse:
        """ Set up any connections required by the handler
        Should return output of check_connection() method after attempting
        connection. Should switch self.is_connected.
        Returns:
            HandlerStatusResponse
        """
    ```

* Implementing the `disconnect()` method:

    The `disconnect()` method closes the existing connection.

    ```py
    def disconnect(self):
        """ Close any existing connections
        Should switch self.is_connected.
        """
        self.is_connected = False
        return self.is_connected
    ```

* Implementing the `check_connection()` method:

    The `check_connection()` method performs the health check for the connection.

    ```py
    def check_connection(self) -> HandlerStatusResponse:
        """ Check connection to the handler
        Returns:
            HandlerStatusResponse
        """
    ```

* Implementing the `native_query()` method:

    The `native_query()` method runs commands of the native database language.

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

* Implementing the `query()` method:

    The query method runs parsed SQL commands.

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

* Implementing the `get_tables()` method:

    The `get_tables()` method lists all the available tables.

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

* Implementing the `get_columns()` method:

    The `get_columns()` method lists all columns of a specified table.

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

### Creating the `connection_args` Dictionary

The `connection_arg` dictionary contains all required arguments to establish the connection.

Here is an example of the `connection_arg` dictionary from the [MySQL handler](https://github.com/mindsdb/mindsdb/tree/staging/mindsdb/integrations/handlers/mysql_handler).

```py
connection_args = OrderedDict(
    user={
        'type': ARG_TYPE.STR,
        'description': 'The user name used to authenticate with the MySQL server.'
    },
    password={
        'type': ARG_TYPE.STR,
        'description': 'The password to authenticate the user with the MySQL server.'
    },
    database={
        'type': ARG_TYPE.STR,
        'description': 'The database name to use when connecting with the MySQL server.'
    },
    host={
        'type': ARG_TYPE.STR,
        'description': 'The host name or IP address of the MySQL server. NOTE: use \'127.0.0.1\' instead of \'localhost\' to connect to local server.'
    },
    port={
        'type': ARG_TYPE.INT,
        'description': 'The TCP/IP port of the MySQL server. Must be an integer.'
    },
    ssl={
        'type': ARG_TYPE.BOOL,
        'description': 'Set it to False to disable ssl.'
    },
    ssl_ca={
        'type': ARG_TYPE.PATH,
        'description': 'Path or URL of the Certificate Authority (CA) certificate file'
    },
    ssl_cert={
        'type': ARG_TYPE.PATH,
        'description': 'Path name or URL of the server public key certificate file'
    },
    ssl_key={
        'type': ARG_TYPE.PATH,
        'description': 'The path name or URL of the server private key file'
    }
)
```

### Creating the `connection_args_example` Dictionary

The `connection_args_example` dictionary contains an example of all required arguments to establish the connection.

Here is an example of the `connection_args_example` dictionary from the [MySQL handler](https://github.com/mindsdb/mindsdb/tree/staging/mindsdb/integrations/handlers/mysql_handler).

```py
connection_args_example = OrderedDict(
    host='127.0.0.1',
    port=3306,
    user='root',
    password='password',
    database='database'
)
```

### Exporting All Required Variables

This is what should be exported in the `__init__.py` file:

- The `Handler` class.
- The `version` of the handler.
- The `name` of the handler.
- The `type` of the handler, either `DATA` handler or `ML` handler.
- The `icon_path` to the file with the database icon.
- The `title` of the handler or a short description.
- The `description` of the handler.
- The `connection_args` dictionary with the connection arguments.
- The `connection_args_example` dictionary with an example of the connection arguments.
- The `import_error` message that is used if the import of the `Handler` class fails.

Let's look at an example of the `__init__.py` file.

```py
...
title = 'Trino'
version = 0.1
description = 'Integration for connection to TrinoDB'
name = 'trino'
type = HANDLER_TYPE.DATA
icon_path = 'icon.png'

__all__ = [
    'Handler', 'version', 'name', 'type', 'title',
    'description', 'connection_args_example', 'icon_path'
]
```

## Check out our Handlers!

To see some integration handlers that are currently in use, we encourage you to check out the following handlers inside the MindsDB repository:

* [MySQL](https://github.com/mindsdb/mindsdb/tree/staging/mindsdb/integrations/handlers/mysql_handler)
* [Postgres](https://github.com/mindsdb/mindsdb/tree/staging/mindsdb/integrations/handlers/postgres_handler)
* [MLflow](https://github.com/mindsdb/mindsdb/tree/staging/mindsdb/integrations/handlers/mlflow_handler)
* [Lightwood](https://github.com/mindsdb/mindsdb/tree/staging/mindsdb/integrations/handlers/lightwood_handler)

And here are [all the handlers available in the MindsDB repository](https://github.com/mindsdb/mindsdb/tree/staging/mindsdb/integrations/handlers).

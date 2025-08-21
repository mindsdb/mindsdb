# MindsDB Handler

This handler allows you to connect to a MindsDB server using the MySQL wire protocol. It extends the functionality of the MySQL handler to specifically work with MindsDB servers and includes support for knowledge bases.

## Features

- Connect to MindsDB servers via MySQL wire protocol
- Execute SQL queries on MindsDB
- List tables including knowledge bases
- Get column information for tables
- SSL support for secure connections

## Connection Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `host` | string | Yes | The host name or IP address of the MindsDB server |
| `port` | integer | Yes | The TCP/IP port of the MindsDB server (default: 3306) |
| `user` | string | Yes | The user name used to authenticate with the MindsDB server |
| `password` | string | Yes | The password to authenticate the user with the MindsDB server |
| `database` | string | Yes | The database name to use when connecting with the MindsDB server |
| `url` | string | No | URI-Like connection string to the MindsDB server (overrides other parameters) |
| `ssl` | boolean | No | Set to True to enable SSL |
| `ssl_ca` | string | No | Path or URL of the Certificate Authority (CA) certificate file |
| `ssl_cert` | string | No | Path name or URL of the server public key certificate file |
| `ssl_key` | string | No | The path name or URL of the server private key file |

## Example Connection

```python
connection_data = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "root",
    "password": "password",
    "database": "mindsdb"
}
```

## Key Differences from MySQL Handler

1. **Knowledge Base Support**: The `get_tables()` method includes "KNOWLEDGE BASE" as a valid table type, allowing you to see knowledge bases alongside regular tables and views.

2. **MindsDB-Specific Logging**: All log messages reference "MindsDB" instead of "MySQL" for clarity.

3. **Handler Name**: The handler is registered as "mindsdb" to distinguish it from the regular MySQL handler.

## Usage

```python
from mindsdb.integrations.handlers.mindsdb_handler import MindsDBHandler

# Create handler instance
handler = MindsDBHandler("mindsdb_connection", connection_data=connection_data)

# Check connection
status = handler.check_connection()
print(f"Connection successful: {status.success}")

# Get tables (including knowledge bases)
tables = handler.get_tables()
print(tables.data_frame)

# Execute a query
result = handler.native_query("SELECT * FROM information_schema.tables")
print(result.data_frame)
```

## Requirements

- `mysql-connector-python==9.1.0`
- `pandas`
- `mindsdb_sql_parser`

## Notes

- Use '127.0.0.1' instead of 'localhost' to connect to local server
- The handler uses the same MySQL connector as the MySQL handler, so it's compatible with any MindsDB server that supports the MySQL wire protocol
- Knowledge bases will appear in the table list with `TABLE_TYPE = 'KNOWLEDGE BASE'`

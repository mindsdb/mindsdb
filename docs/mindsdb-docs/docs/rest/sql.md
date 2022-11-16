# `#!sql /api/sql/query` Endpoint

## Description

This API provides a REST endpoint for executing the SQL queries. Note:

* This endpoint is a HTTP POST method.
* This endpoint accept data via `application/json` request body. 
* The only required key is the `query` which has the SQL statement value.

## Syntax


```json
POST http://{{url}}}/api/sql/query

{
"query": "The SQL Query you want to execute"
}
```

On execution, we get `Status 200 OK`:

```json
{
   "context": {},
   "type": "ok"
}
```

## Example


=== "MindsDB Cloud"

    ```json
    POST https://cloud.mindsdb.com/api/sql/query

    {
    "query": "SELECT sqft, rental_price FROM example_db.demo_data.home_rentals LIMIT 10;"
    }
    ```

    On execution, we get:

    ```json
    {
    "column_names": [
        "sqft",
        "rental_price"
    ],
    "context": {
        "db": "mindsdb"
    },
    "data": [
        [
            917,
            3901
        ],
        [
            194,
            2042
        ]
    ],
    "type": "table"
    }
    ```

=== "Local MindsDB"

    ```json    
    POST http://127.0.0.1:47334/api/sql/query

    {
    "query": "SELECT sqft, rental_price FROM example_db.demo_data.home_rentals LIMIT 10;"
    }
    ```

    On execution, we get:

    ```json
    {
    "column_names": [
        "sqft",
        "rental_price"
    ],
    "context": {
        "db": "mindsdb"
    },
    "data": [
        [
            917,
            3901
        ],
        [
            194,
            2042
        ]
    ],
    "type": "table"
    }
    ```
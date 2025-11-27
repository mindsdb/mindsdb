# PocketBase Handler

## Table of Contents
- [About](#about)
- [Handler Implementation](#handler-implementation)
  - [Setup](#setup)
  - [Connection](#connection)
  - [Required Parameters](#required-parameters)
  - [Optional Parameters](#optional-parameters)
- [Example Usage](#example-usage)
- [Supported Tables/Tasks](#supported-tablestasks)
- [Limitations](#limitations)
- [TODO](#todo)

## About
The PocketBase handler lets MindsDB read from and write to [PocketBase](https://pocketbase.io/) collections by exposing them as SQL tables. Once connected, each collection becomes queryable through the MindsDB SQL API, enabling analysts to join PocketBase data with other sources and run predictive workflows without leaving SQL.

## Handler Implementation

### Setup
1. Install and run PocketBase following the [official guide](https://pocketbase.io/docs/).  
2. Create an admin user (or another account with permissions to access the target collections).  
3. Make the PocketBase instance reachable from the MindsDB server (for development `http://127.0.0.1:8090` is typical).  
4. Ensure the admin credentials you plan to use have access to every collection that MindsDB should expose.

### Connection
Use the `CREATE DATABASE` statement to register the handler. All statements follow standard MindsDB SQL syntax:

```sql
CREATE DATABASE pocketbase_conn
WITH ENGINE = "pocketbase",
PARAMETERS = {
    "url": "http://127.0.0.1:8090",
    "email": "admin@example.com",
    "password": "supersecret",
    "collections": ["posts", "users"]
};
```

After the database is created you can query a collection with a `SELECT` statement, or insert/update/delete records just like any other table exposed through MindsDB.

### Required Parameters
- `url`: Full base URL of the PocketBase instance (including protocol and port).  
- `email`: Admin (or service) email used during authentication.  
- `password`: Password for the supplied email. This value is stored as a secret inside MindsDB metadata.

### Optional Parameters
- `collections`: List of collection names to expose. When omitted, the handler registers every collection returned by `/api/collections`.

## Example Usage

```sql
-- Read the latest posts
SELECT id, title, published_at
FROM pocketbase_conn.posts
ORDER BY published_at DESC
LIMIT 10;

-- Insert a record
INSERT INTO pocketbase_conn.posts (title, body, published_at)
VALUES ('Hello MindsDB', 'PocketBase handler demo', NOW());

-- Update a record (updates require filtering by id)
UPDATE pocketbase_conn.posts
SET title = 'Updated title'
WHERE id = 'rec123';

-- Delete a record (deletes also require the id)
DELETE FROM pocketbase_conn.posts
WHERE id = 'rec123';
```

## Supported Tables/Tasks
- Automatic table registration for every accessible collection.
- `SELECT`, `INSERT`, `UPDATE`, and `DELETE` statements against collections.
- `SHOW TABLES` / `DESCRIBE table` style metadata through `get_tables` and `get_columns`.
- Projection, sorting, and filtering pushed down to PocketBase when possible, with remaining logic evaluated inside MindsDB.

## Limitations
- Authentication currently uses the admin password flow. Custom auth providers (OAuth, collection-based auth) are not yet supported.
- Remote filtering only covers basic comparison operators. Complex expressions, `IN`, and pattern searches fall back to in-memory filtering.
- `UPDATE` and `DELETE` statements must include an `id` filter; bulk operations over arbitrary filters are not supported yet.
- PocketBase file uploads are outside the scope of this handler.

## TODO
1. Support collection auth providers and token-based authentication flows.
2. Push down more SQL operators (IN, LIKE, BETWEEN) to the PocketBase filtering language.
3. Add automated tests that mock the PocketBase API responses.
4. Surface collection schema metadata (field types, constraints) through MindsDB's catalog APIs.

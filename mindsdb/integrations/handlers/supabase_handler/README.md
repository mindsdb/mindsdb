# Supabase Handler

This is the implementation of the Supabase handler for MindsDB.

## Supabase

Supabase is an open source Firebase alternative. Start your project with a Postgres Database, Authentication, instant APIs, Realtime subscriptions and Storage.

## Implementation

This handler was implemented by extending postres connector.

The required arguments to establish a connection are:

* `host`: the host name of the Supabase connection 
* `port`: the port to use when connecting 
* `user`: the user to authenticate 
* `password`: the password to authenticate the user
* `database`: database name

## Usage

In order to make use of this handler and connect to a Supabase server in MindsDB, the following syntax can be used:

```sql
CREATE DATABASE example_supabase_data
WITH ENGINE = "supabase",
PARAMETERS = { 
  "user": "root",
  "password": "root",
  "host": "hostname",
  "port": "5432",
  "database": "postgres"
}
```

Now, you can use this established connection to query your database as follows:

```sql
SELECT * FROM example_supabase_data.public.rentals LIMIT 10;
```
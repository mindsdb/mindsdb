# Postgres API

## Description

The Postgres API enables users to connect to MindsDB using the PostgreSQL Protocol, in much the same way you can connect to MindsDB using MySQL.

## How To Run

To run mindsdb normally, with the Postgres API alongside it, you can run

```commandline
python -m mindsdb --api http,mysql,postgres
```
If instead you only want to run the Postgres API, you can run

```commandline
python -m mindsdb --api postgres
```

To connect using Postgres, you can use the CLI program psql (clients like DBeaver aren't supported yet. See Implementation details)
Use the mindsdb user, with an empty password or lack there of.  

```commandline
psql -h localhost -p 55432 -d mindsdb -U mindsdb
```

## Current Implementation
### Current Support
There are many things that aren't supported yet. 

Right now you can use psql to connect to a local mindsdb instance.
You can also use psycopg and other clients, although DBeaver is finicky.

You can run simple one off queries using this psql client, and BEGIN COMMIT blocks should work. Dialect support for Postgres is ongoing, we currently back up into our generic SQL / MySQL parser.

### Remaining Implementation Goals

Enough Protocol Support
: Right now we are not responding well to things like BIND, PARSE, DESCRIBE, etc.
: We are faking our version as well. This was assigned pretty arbitrarily
: SSL Support is needed.

Cloud / Dependent Code
: Support still needs to be added internally for cloud.

Dialect Support
: Support for Postgresql dialect needs to be more fleshed out.
: There is also more additions needed to mindsdb_sql to support the Postgresql Dialect
: These changes go hand in hand with some protocol functionalities around transactions. 



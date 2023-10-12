# Surrealdb Handler

This is the implementation of the Surrealdb handler for MindsDB.

## Surrealdb

SurrealDB is an innovative NewSQL cloud database, suitable for serverless applications,
jamstack applications, single-page applications, and traditional applications.
It is unmatched in its versatility and financial value, with the ability for deployment on cloud,
on-premise, embedded, and edge computing environments.

## Implementation

This handler was implemented by using the python library `pysurrealdb`.

The required arguments to establish a connection are:

* `host`: the host name of the Surrealdb connection
* `port`: the port to use when connecting
* `user`: the user to authenticate
* `password`: the password to authenticate the user
* `database`: database name to be connected
* `namespace`: namespace name to be connected

## Usage

In order to make use of this handler and connect to a SurrealDB server. First you need to have [SurrealDB](https://surrealdb.com/install) installed and once you have it installed.

To use SurrealDB we have to start the SurrealDB server in our local environment. For that you need to give following command in the terminal:
```
surreal start --user root --pass root
```

This will start the server and start accepting requests from port `8000`. Now, in another terminal session, give the following command:
```
surreal sql --conn http://localhost:8000  \
--user root --pass root --ns testns --db testdb
```

This will create a namespace `testns` for your project and a database `testdb` in order to proceed further.

Here, let's create a table in our newly created database with the following:
```
CREATE dev SET name='again', status='founder';
```

This will create a table named `dev` with column `name` and `status`.

(If you want to use SurrealDB in public cloud editor, feel free to skip the following steps.)

## Testing SurrealDB in the local environment

Use the following query to create a SurrealDB database in the MindsDB environment.

```sql
CREATE DATABASE exampledb
WITH ENGINE = 'surrealdb',
PARAMETERS = {
  "host": "localhost",
  "port": "8000",
  "user": "root",
  "password": "root",
  "database": "testdb",
  "namespace": "testns"
};
```

Now, you can use this established connection to query your database tables as follows:

```sql
SELECT * FROM exampledb.dev;
```

## Testing SurrealDB in the public cloud environment

To establish a connection with our SurrealDB server which is running locally to the public cloud instance is not that simple. We are going to use `ngrok tunneling` to connect cloud instance to the local SurrealDB server. You can follow this [guide](https://docs.mindsdb.com/sql/create/database#making-your-local-database-available-to-mindsdb) for that.

In our case with `ngrok` we will use:
```
ngrok tcp 8000
```

From there, it generated a forwarding dns for me which is:
```
tcp://6.tcp.ngrok.io:17141 -> localhost:8000
```

It will be different in your case. With this let's connect to the public cloud using

```sql
CREATE DATABASE exampledb
WITH ENGINE = 'surrealdb',
PARAMETERS = {
  "host": "6.tcp.ngrok.io",
  "port": "17141",
  "user": "root",
  "password": "root",
  "database": "testdb",
  "namespace": "testns"
};
```

Please change the `host` and `port` properties in the `PARAMETERS` clause based on the values which you got.

We can also query the `dev` table which we created with
```sql
SELECT * FROM exampledb.dev;
```
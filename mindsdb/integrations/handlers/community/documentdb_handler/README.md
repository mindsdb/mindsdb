## Implementation

This handler is implemented by extending the MongoDBHandler.

The required arguments to establish a connection are as follows:

-   `username` is the database user.
-   `password` is the database password.
-   `host` is the host IP address or URL.
-   `port` is the port used to make TCP/IP connection.
-   `database` is the database name.

There are several optional arguments (refer https://pymongo.readthedocs.io/en/stable/api/pymongo/mongo_client.html) that can be used as well and can be passed as kwargs:

-   `tls` indicates whether tls is enabled (`True`) or disabled (`False`).
-   `serverSelectionTimeoutMS` Controls how long (in milliseconds) the driver will wait to find an available.
-   `directConnection` if `True`, forces this client to connect directly to the specified DocumentDB host as a standalone. If `False`, the client connects to the entire replica set of which the given DocumentDB host(s) is a part.
-   `tlsAllowInvalidHostnames` If `True`, disables TLS hostname verification. `False` implies tls=True.
-   `tlsAllowInvalidCertificates` If `True`, continues the TLS handshake regardless of the outcome of the certificate verification process.
-   `retryWrites` Whether supported write operations executed within this MongoClient will be retried once after a network error.
-   `tlsCAFile` A file containing a single or a bundle of “certification authority” certificates, which are used to validate certificates passed from the other end of the connection.

## Usage

In order to make use of this handler and connect to the DocuementDB database in MindsDB, the following syntax can be used:

```sql
  db.databases.insertOne({
      name: "example_documentdb",
      engine: "documentdb",
      connection_args: {
          "username": "username",
          "password": "password",
          "host": "127.0.0.1",
          "port": "27017",
          "database": "sample_database",
          "kwargs": {
              "directConnection": true,
              "serverSelectionTimeoutMS": 2000,
              "tls": true,
              "tlsAllowInvalidHostnames": true,
              "tlsAllowInvalidCertificates": true,
              "retryWrites": false,
              "tlsCAFile": "/home/global-bundle.pem"
          }
      }
  });
```

You can use this established connection to query your table as follows.

```sql
  use example_documentdb;
  show collections;
  db.sample_collection.find({});
```

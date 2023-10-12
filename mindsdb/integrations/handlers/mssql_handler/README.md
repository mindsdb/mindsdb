# Microsoft SQL Server Handler

This is the implementation of the Microsoft SQL Server data handler for MindsDB.

## Microsoft SQL Server

[Microsoft SQL Server](https://www.microsoft.com/en-us/sql-server) is a relational database management system developed by Microsoft. As a database server, it stores and retrieves data as requested by other software applications, which may run either on the same computer or on another computer across a network.

## Implementation

This handler is implemented using `pymssql`, the Python language extension module that provides access to Microsoft SQL Server from Python scripts.

The required arguments to establish a connection are as follows:

* `host` is the host name or IP address.
* `port` is the port used to make TCP/IP connection.
* `database` is the database name.
* `user` is the database user.
* `password` is the database password.

If you installed MindsDB locally via pip, you need to install all handler dependencies manually. To do so, go to the handler's folder (mindsdb/integrations/handlers/mssql_handler) and run this command:   `pip install -r requirements.txt`.

## Installation

We are going to first install Microsoft SQL Server locally using docker, please follow along:
```
sudo docker pull mcr.microsoft.com/mssql/server:2022-latest
```

With this, we will get the Microsoft SQL Server docker image. Now, to run this docker image, we will use:

```
sudo docker run -e "ACCEPT_EULA=Y" -e "MSSQL_SA_PASSWORD=admin5678@" \
   -p 1433:1433 --name test --hostname mssql \
   -d \
   mcr.microsoft.com/mssql/server:2022-latest
```

Here we have given following values:
- `password`: 'admin5678@'
- `port`: '1433'
- `username`: 'test'
- `hostname`: 'mssql'

We have our Microsoft SQL Server running, now open your terminal and give following commands:

```
sudo docker exec -it test "bash"
```

This command will let us get in the running container, from there we will login with our credentials and create DB Tables.

We can login by passing the Server, Username, and Password:
```
/opt/mssql-tools/bin/sqlcmd -S mssql -U SA
```

It will then prompt for the password which you have created. If successful, you should get to a sqlcmd command prompt: 1>.

We have successfully created the local running database server.

Let's create a database and a table with few columns.
```sql
CREATE DATABASE TestDB
```

```sql
USE TestDB;
```

You won't see any output after writing these queries. Because, they don't run immediately. If you want to execute these, write `GO` in the next line to execute all the previous commands.

```sql
CREATE TABLE Inventory (id INT, name NVARCHAR(50), quantity INT);
```

```sql
INSERT INTO Inventory VALUES (1, 'banana', 150); 
INSERT INTO Inventory VALUES (2, 'orange', 154);
```

```sql
SELECT * FROM Inventory;
```

```sql
GO
```

## Usage

In order to make use of this handler with SQL Server running locally with MindsDB, we have to use `ngrok tunneling`, please follow this [guide](https://docs.mindsdb.com/sql/create/database#making-your-local-database-available-to-mindsdb) to achieve that:

In our case we will write:
```
ngrok tcp 1433
```

Suppose the forwarding ports are:
```
tcp://0.tcp.ngrok.io:10985 -> localhost:1433
```

So, we can use this to create DATABASE using MindsDB

```sql
CREATE DATABASE mssql_datasource
WITH
    engine = 'mssql',
    parameters = {
      "host": "0.tcp.ngrok.io",
      "port": 10985,
      "database": "TestDB",
      "user": "SA",
      "password": "admin5678@"
    };
```

You can use this established connection to query your table as follows:

```sql
SELECT *
FROM mssql_datasource.Inventory;
```
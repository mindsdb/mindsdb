# Connect your data

!!! tip "The recommended way to connect MindsDB to your data store is through MindsDB Studio."
    
    Below is a list of all the integrations we currently support:

    | SQL | NoSQL | Streams | Data Warehouse|
    |-| -| -| -|
    | <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/MySQL-00758F?style=for-the-badge&logo=mysql&logoColor=white" alt="Connect MySQL"></a> | <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/MongoDB-4EA94B?style=for-the-badge&logo=mongodb&logoColor=white" alt="Connect MongoDB"></a> | <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/redis-%23DD0031.svg?&style=for-the-badge&logo=redis&logoColor=white" alt="Connect Redis"></a> | <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/Snowflake-35aedd?style=for-the-badge&logo=snowflake&logoColor=blue" alt="Connect Snowflake"></a> |
    | <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white" alt="Connect PostgreSQL"></a> | <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/ScyllaDB-F4F5FF?style=for-the-badge&logo=scylladb&logoColor=white" alt="Connect ScyllaDB"></a>  |  <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/Apache Kafka-808080?style=for-the-badge&logo=apache-kafka&logoColor=white" alt="Connect Kafka"></a> | -|
    | <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/MariaDB-003545?style=for-the-badge&logo=mariadb&logoColor=white" alt="Connect MariaDB"></a> | -| -| -|
    | <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/Clickhouse-e6e600?style=for-the-badge&logo=clickhouse&logoColor=white" alt="Connect Clickhouse"></a> |-| -| -|
    | <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/Cassandra-1287B1?style=for-the-badge&logo=apache%20cassandra&logoColor=white" alt="Connect Cassandra"></a> |-| -| -|
    | <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/Microsoft%20SQL%20Sever-CC2927?style=for-the-badge&logo=microsoft%20sql%20server&logoColor=white" alt="Connect SQL Server"></a> | -| -| -|
    | <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/Singlestore-5f07b4?style=for-the-badge&logo=singlestore&logoColor=white" alt="Connect Singlestore"></a> | -| -| -|
    | <a href="https://docs.mindsdb.com/"><img src="https://img.shields.io/badge/CockroachDB-585AF4?style=for-the-badge&logo=cockroachdb&logoColor=white" alt="Connect CockroachDB"></a> | -| -| -|

    If you want to connect without using the MindsDB Studio, you can find the specific database configuration examples [here](/datasources/configuration/#extending-default-configuration).

#### Connect to database

1. From the left navigation menu, select Database Integration.
2. Click on the `ADD DATABASE` button.
3. In the `Connect to Database` modal window:
    1. Select the Supported Database that you want to connect to.
    2. Add the Integration name(how you want to name the integration between your database and MindsDB).
    3. Add the Database name.
    4. Add the Hostname.
    5. Add Port.
    6. Add the database user.
    7. Add Password for the user.
    8. Click on `CONNECT`.

![Connect to database](/assets/data/mysql.gif)

!!! tip "Required inputs"
    Note: For different type of database there could be different required inputs you need to provide.

After connecting MindsDB and the database, use your SQL client to [connect to MindsDB as a database](/sql/connect/cloud/).



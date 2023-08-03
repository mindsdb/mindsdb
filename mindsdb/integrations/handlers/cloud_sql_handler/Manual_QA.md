# Welcome to the MindsDB Manual QA Testing for Google Cloud SQL Handler

## Testing Google Cloud SQL Handler

**1. Testing CREATE DATABASE**


```sql
CREATE DATABASE cloud_sql_mysql_datasource
WITH ENGINE = 'cloud_sql',
PARAMETERS = {
    "db_engine": "mysql",
    "host": "34.65.85.18",
    "port": 3306,
    "user": "mindsdbtest2",
    "password": "oi0Fi=u_YNM&agO`",
    "database": "information_schema"
};
```
The result is as follows:
[![Screenshot-from-2023-07-20-16-31-02.png](https://i.postimg.cc/przC5PbH/Screenshot-from-2023-07-20-16-31-02.png)](https://postimg.cc/7JP0p8Zs)

**2. Testing Select method**
```sql
SELECT *
FROM cloud_sql_mysql_datasource.COLUMNS;
```
The result is as follows:
[![google-cloud-select-method.png](https://i.postimg.cc/DwpBJjJB/google-cloud-select-method.png)](https://postimg.cc/zyWn4kkg)
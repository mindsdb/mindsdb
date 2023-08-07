# Welcome to the MindsDB Manual QA Testing for the Apache Ignite Handler

## Testing the Apache Ignite Handler

**1. Testing CREATE DATABASE**

```sql
CREATE DATABASE ignite_datasource
WITH ENGINE = 'ignite',
PARAMETERS = {
    "host": "0.tcp.ap.ngrok.io",
    "port": 12593
};
```

The result is as follows:
<br>
[![ignite-create-db.png](https://i.postimg.cc/HLSByMqt/ignite-create-db.png)](https://postimg.cc/947d3DF4)

**2. Testing SELECT method**

```sql
SELECT * FROM ignite_datasource.City LIMIT 10;
```

The result is as follows:
<br>
[![ignite-select.png](https://i.postimg.cc/85jwB5Dg/ignite-select.png)](https://postimg.cc/cgGQddrF)
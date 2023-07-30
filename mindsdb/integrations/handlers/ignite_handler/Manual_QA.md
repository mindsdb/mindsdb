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

**2. Testing SELECT method**

```sql
SELECT * FROM ignite_datasource.City LIMIT 10;
```

The result is as follows:
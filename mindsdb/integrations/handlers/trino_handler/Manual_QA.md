# Welcome to the MindsDB Manual QA Testing for TrinoDB Handler

## Testing TrinoDB Handler 

**1. Testing CREATE DATABASE**

```
CREATE DATABASE trino_datasource
WITH
  ENGINE = 'trino',
  PARAMETERS = {
    "host": "localhost",
    "port": 8080,
    "auth": "basic",
    "http_scheme": "http",
    "user": "trino",
    "catalog": "tpch",
    "schema": "sf1",
    "with": "with (transactional = true)"
  };
```

[![create-database-trino.png](https://i.postimg.cc/mZ5kYfwC/create-database-trino.png)](https://postimg.cc/9zyc2ksQ)


**2. Testing SELECT FROM PREDICTOR**

```
SELECT *
FROM trino_datasource.customer;
```

[![select-trinodb.png](https://i.postimg.cc/s2xDQThw/select-trinodb.png)](https://postimg.cc/bGXj4HLt)

### Results

Drop a remark based on your observation.
- [x] Works Great 💚 (This means that all the steps were executed successfuly and the expected outputs were returned.)

---
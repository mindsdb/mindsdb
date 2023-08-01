# Welcome to the MindsDB Manual QA Testing for Supabase Handler


## Testing Supabase Handler 

**1. Testing CREATE DATABASE**

```
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

The result is as follows:
[![supabase-connection.png](https://i.postimg.cc/nhdpTmdf/supabase-connection.png)](https://postimg.cc/DSb9wSkx)


**2. Testing Select method**

```
SELECT *
FROM example_supabase_data.test;
```

The result is as follows:
[![supabase-select.png](https://i.postimg.cc/kgRfh3sM/supabase-select.png)](https://postimg.cc/NKY65nZZ)


**3. Testing Insert method**


```
INSERT INTO example_supabase_data.test (id, created_at)
VALUES (1, '2021-07-20 16:00:00');
```

[![supabase-insert.png](https://i.postimg.cc/L8ZFkWZL/supabase-insert.png)](https://postimg.cc/xXYZnt51)

### Results

Drop a remark based on your observation.
- [x] Works Great 💚 (This means that all the steps were executed successfuly and the expected outputs were returned.)
---
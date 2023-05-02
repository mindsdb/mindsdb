# Welcome to the MindsDB Manual QA Testing for SAP MaxDB server Handler

---

## Testing SAP MaxDB with [Africas Economy Data](https://github.com/marsidmali/mm/files/11347260/africa.csv)

**1. Testing CREATE DATABASE**

```sql
CREATE DATABASE global_economy    --- display name for database.
WITH engine='maxdb',              --- name of the mindsdb handler
parameters={
    "host": "127.0.0.1",          --- host, it can be an ip or an url
    "port": 7210,                 --- common port is 7210.
    "user": "DBADMIN",            --- Your database user.
    "password": "J312Mnsns",      --- Your password.
    "database": "MAXDB",          --- Your database name.
    "jdbc_location": "/Program Files/sdb/MALI/runtime/jar/sapdbc.jar"  --- The location of the jar file which contains the JDBC driver
};
```

![image](https://user-images.githubusercontent.com/93339789/235527864-7d1ea5c1-7f60-4015-9eba-fed42efbab11.png)

**2. Testing CREATE PREDICTOR**

```sql
CREATE PREDICTOR mindsdb.africa
FROM global_economy
    (SELECT * FROM africa)
PREDICT INFLATION_RATE;
```

![image](https://user-images.githubusercontent.com/93339789/234972889-49fa8ad1-9472-4874-9149-af86d3cf1ecc.png)

**3. Testing SELECT FROM PREDICTOR**

```sql
SELECT *
FROM mindsdb.predictors
WHERE name='africa';
```

![image](https://user-images.githubusercontent.com/93339789/234973194-18ddd80a-9600-47ef-8357-1ef16f31d65e.png)
### Results

- [x] Works Great 💚

---

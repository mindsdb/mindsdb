# Welcome to the MindsDB Manual QA Testing for Postgres Handler

> **Please submit your PR in the following format after the underline below `Results` section. Don't forget to add an underline after adding your changes i.e., at the end of your `Results` section.**

## Testing Postgres Handler with [Dataset Name](URL to the Dataset)

**1. Testing CREATE DATABASE**

```
--- After you fill all the mandatory fields run the query with the top button or shift + enter. 
    
CREATE DATABASE postgres_test  --- display name for database. 
WITH ENGINE = 'postgres',     --- name of the mindsdb handler 
PARAMETERS = {
    "user": "postgres",              --- Your database user.
    "password": "changeme",          --- Your password.
    "host": "192.168.1.163",              --- host, it can be an ip or an url. 
    "port": "5432",           --- common port is 5432.
    "database": "DIABETES_DATA"           --- The name of your database *optional.
};
```

![CREATE_DATABASE](https://user-images.githubusercontent.com/241893/195336804-c14d06dd-ef40-4e2b-875b-2deb35fa64bb.png)
)

**2. Testing CREATE PREDICTOR**

```
CREATE PREDICTOR mindsdb.diabetes_predictor
FROM postgres_test (
    SELECT * FROM diabetes
) PREDICT class;
```

![CREATE_PREDICTOR](https://user-images.githubusercontent.com/241893/195336892-0e60ed8a-c339-4b93-8bca-12c135609f06.png)
)

**3. Testing SELECT FROM PREDICTOR**

```
SELECT *
FROM mindsdb.models
WHERE name='diabetes_predictor';
```

![SELECT_FROM](https://user-images.githubusercontent.com/241893/195337026-7d80d631-bb8a-445b-bc68-5ee7788de2d9.png)
)

### Results

Drop a remark based on your observation.
- [X] Works Great 💚 (This means that all the steps were executed successfuly and the expected outputs were returned.)
- [ ] There's a Bug 🪲 [Issue Title](URL To the Issue you created) ( This means you encountered a Bug. Please open an issue with all the relevant details with the Bug Issue Template)

Looking forward to further testing / development.

---

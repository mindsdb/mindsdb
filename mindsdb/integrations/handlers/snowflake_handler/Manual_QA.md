# Welcome to the MindsDB Manual QA Testing for Snowflake Handler

## Testing Snowflake Handler with [Titanic Dataset](https://www.kaggle.com/datasets/brendan45774/test-file)

**1. Testing CREATE DATABASE**

```
CREATE DATABASE TITANIC  
WITH ENGINE = 'snowflake',    
PARAMETERS = {
  "user": "MindsDB",                --- Your database user.
  "account": "cq58025",             --- Snowflake account.
  "warehouse": "COMPUTE_WH",           --- warehouse account.
  "protocol": "https",            --- Common https
  "database": "DUMMY_DATABASE",             --- The name of your database
  "schema": "PUBLIC",              --- common schema PUBLIC.
  "password": "MindsDB@12345",            --- Your password.
  "host": "cq58025.ap-southeast-1.snowflakecomputing.com",                --- host, it can be an ip or an url.
  "port": "443"              --- common port is 443.
};
```

![CREATE_DATABASE](https://github.com/NishitSingh2023/mindsdb/assets/43803790/02d5fad0-b85e-4f9c-9e93-2d1ef9436d88)


**2. Testing CREATE MODEL**

```
CREATE MODEL 
  mindsdb.titanic_prediction_model
FROM TITANIC
  (SELECT * FROM TITANIC)
PREDICT Survived;
```

![CREATE_PREDICTOR](https://github.com/NishitSingh2023/mindsdb/assets/43803790/5067b931-5715-4f4f-a24d-6780b0eb13a6)


**3. Testing SELECT FROM MODEL**

```
SELECT *
FROM mindsdb.models
WHERE name='titanic_prediction_model';
```

![SELECT_FROM](https://github.com/NishitSingh2023/mindsdb/assets/43803790/26dc1b6b-9370-4682-8e94-1df8c407d68e)


**4. Testing DROP THE DATABASE**

```
drop database TITANIC;
```

![DROP_DB](https://github.com/NishitSingh2023/mindsdb/assets/43803790/820f6837-6a39-4489-86c4-2f5911666ca2)




### Results

Drop a remark based on your observation.
- [x] Works Great 💚 (This means that all the steps were executed successfuly and the expected outputs were returned.)
- [ ] There's a Bug 🪲 [Issue Title](URL To the Issue you created) ( This means you encountered a Bug. Please open an issue with all the relevant details with the Bug Issue Template)

---

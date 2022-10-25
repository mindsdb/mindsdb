# Welcome to the MindsDB Manual QA Testing for DB2 Handler


## Testing DB2 Handler with [Lung Cancer Dataset](https://www.kaggle.com/datasets/yusufdede/lung-cancer-dataset)

**1. Testing CREATE DATABASE**

```sql
CREATE DATABASE cancer
WITH
engine-'db2',
parameters={
    "user": "db2admin",
    "password" : "1234",
    "host": "127.0.0.1",
    "port": 25000,
    "schema_name": "db2admin"
    "database": "cancer"
};
```

![CREATE_DATABASE](https://user-images.githubusercontent.com/75653580/197382221-b0b9df33-0383-4f26-a639-c9842f92bed8.png)

**2. Testing CREATE PREDICTOR**

```sql
CREATE PREDICTOR mindsdb.lung_cancer_model
FROM cancer (SELECT * FROM lung_cancer)
PREDICT result;

```

![CREATE_PREDICTOR](https://user-images.githubusercontent.com/75653580/197383254-f9b5dbbb-a223-4bc3-b2f9-1f40ab958865.png)

**3. Testing SELECT FROM PREDICTOR**

```sql
SELECT result, result_explain
FROM
mindsdb.lung_cancer_model
WHERE
Alcohol=5 AND smokes=0 AND age=20;
```

![SELECT_FROM](https://user-images.githubusercontent.com/75653580/197382016-64c59de8-7f1d-4efd-a438-9f8537029a5c.png)

### Results

Drop a remark based on your observation.
- [x] Works Great 💚 (This means that all the steps were executed successfuly and the expected outputs were returned.)
- [ ] There's a Bug 🪲 [Issue Title](URL To the Issue you created) ( This means you encountered a Bug. Please open an issue with all the relevant details with the Bug Issue Template)

---
# Welcome to the MindsDB Manual QA Testing for Singlestore Handler

> **Please submit your PR in the following format after the underline below `Results` section. Don't forget to add an underline after adding your changes i.e., at the end of your `Results` section.**

## Testing Singlestore Handler with [Dataset Name](URL to the Dataset)

**1. Testing CREATE DATABASE**

```
COMMAND THAT YOU RAN TO CREATE DATABASE.
```

![CREATE_DATABASE](Image URL of the screenshot)

**2. Testing CREATE PREDICTOR**

```
COMMAND THAT YOU RAN TO CREATE PREDICTOR.
```

![CREATE_PREDICTOR](Image URL of the screenshot)

**3. Testing SELECT FROM PREDICTOR**

```
COMMAND THAT YOU RAN TO DO A SELECT FROM.
```

![SELECT_FROM](Image URL of the screenshot)

### Results

Drop a remark based on your observation.
- [ ] Works Great 💚 (This means that all the steps were executed successfuly and the expected outputs were returned.)
- [ ] There's a Bug 🪲 [Issue Title](URL To the Issue you created) ( This means you encountered a Bug. Please open an issue with all the relevant details with the Bug Issue Template)

---

## Testing Singlestore Handler with [Predictive Maintenance](https://www.kaggle.com/datasets/tolgadincer/predictive-maintenance?select=train.csv)

**1. Testing CREATE DATABASE**

```sql
CREATE DATABASE predictiveMaintenance WITH engine='singlestore',
PARAMETERS = {
  'user': 'admin',
  'password': 'MJFn{VOoaKhr._Hl:[._',
  'host': 'svc-123ae0c5-21f7-4541-840d-7ad6e0f2d5e9-dml.aws-virginia-4.svc.singlestore.com',
  'database': 'singlestore_mindsdb',
  'port': '3306'
};
```

<!-- add screenshot 1 -->
![Screenshot 1](https://i.imgur.com/ezeuAry.png)

**2. Testing CREATE PREDICTOR**

```sql
CREATE PREDICTOR mindsdb.machine_failure_model FROM
predictiveMaintenance.train
(
  SELECT * FROM train LIMIT 10000
)
PREDICT machine_failure;
```

<!-- add screenshot 2 -->
![Screenshot 2](https://i.imgur.com/h3kgeTj.png)

**3. Testing SELECT FROM PREDICTOR**

```sql
SELECT machine_failure FROM mindsdb.machine_failure_model WHERE torque_nm = 40;
```

<!-- add screenshot 3 -->
![Screenshot 3](https://i.imgur.com/knPd6Ox.png)

### Results

Drop a remark based on your observation.
- [] Works Great 💚 (This means that all the steps were executed successfuly and the expected outputs were returned.)
- [x] There's a Bug 🪲 [SingleStore Testing SELECT FROM PREDICTOR produces an error 'query() got an unexpected keyword argument 'query''](https://github.com/mindsdb/mindsdb/issues/3938) 

---

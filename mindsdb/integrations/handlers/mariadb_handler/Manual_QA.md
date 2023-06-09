# Welcome to the MindsDB Manual QA Testing for MariaDB Handler

> **Please submit your PR in the following format after the underline below `Results` section. Don't forget to add an underline after adding your changes i.e., at the end of your `Results` section.**

## Testing MariaDB Handler with [Dataset Name](URL to the Dataset)

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


## Testing MariaDB Handler with [All Nobel Laureates 1901-Present](https://www.kaggle.com/datasets/prithusharma1/all-nobel-laureates-1901-present?resource=download)

**1. Testing CREATE DATABASE**

```
CREATE DATABASE mysql_datasource
WITH ENGINE = 'mariadb'
PARAMETERS = {
  "user": "root",
  "port": 11433,
  "password": "[password]",
  "host": "0.tcp.ngrok.io",
  "database": "mysql"
};
```

![CREATE_DATABASE](https://github.com/wunzt/mindsdb/assets/102569472/92a5ce8f-a98d-42dd-8aab-2a35a9859abd)


**2. Testing CREATE PREDICTOR**

```
CREATE MODEL mindsdb.nobel_model2
FROM mysql_datasource
(SELECT * FROM mysql_datasource.nobel_latest2)
PREDICT prize_share;
```

![CREATE_PREDICTOR](https://github.com/wunzt/mindsdb/assets/102569472/74075bd0-1735-450f-aefd-10f1f92039d6)


**3. Testing SELECT FROM PREDICTOR**

```
SELECT category
FROM mindsdb.nobel_model2
WHERE category='peace';
```

(No Image Due To Error During CREATE PREDICTOR)

**4. Testing DROP DATABASE**

```
DROP DATABASE mysql_datasource;
```

![DROP DATABASE](https://github.com/wunzt/mindsdb/assets/102569472/c3035e7d-1f03-4ecb-bc19-8224157cc01a)

### Results

- [ ] The CREATE DATABASE and DROP DATABASE queries functioned as expected.
- [ ] There's a Bug in the CREATE MODEL query 🪲 [[Bug]: [Manual QA] Test MariaDB Handler Manually: CREATE MODEL Error](https://github.com/mindsdb/mindsdb/issues/6530).
- [ ] Unable to run SELECT FROM PREDICTOR query due to bug in CREATE MODEL query.

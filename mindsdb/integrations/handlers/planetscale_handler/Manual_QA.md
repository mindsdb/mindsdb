# Welcome to the MindsDB Manual QA Testing for PlanetScale Handler

> **Please submit your PR in the following format after the underline below `Results` section. Don't forget to add an underline after adding your changes i.e., at the end of your `Results` section.**

## Testing PlanetScale Handler with [Raisin Dataset](https://www.kaggle.com/datasets/muratkokludataset/raisin-dataset)
### Results
**1. Testing CREATE DATABASE**

```sql
CREATE DATABASE planetscale_test            --- display name for database
WITH ENGINE = 'planet_scale',               --- name of the MindsDB handler 
PARAMETERS = {
  "host": "cloud.mindsdb.com",              --- host to server IP Address or hostname
  "port": "3306",                           --- port through which TCP/IP connection is to be made
  "user": "test_user",                      --- username associated with database
  "password": "test_pswd",                  --- password to authenticate your access
  "database": "files"                       --- database name to be connected
};
```

![CREATE_DATABASE](create-db.png)

**2. Testing CREATE PREDICTOR**

```sql
CREATE PREDICTOR mindsdb.raisin_predictor
FROM planetscale_test (
    SELECT * FROM raisin_data
) PREDICT Class;
```

![CREATE_PREDICTOR](create-predictor.png)

**3. Testing SELECT FROM PREDICTOR**

```sql
SELECT * 
FROM mindsdb.models
WHERE name='raisin_predictor';
```

![SELECT_FROM_PREDICTOR](predict-target.png)

**4. Testing DROP THE DATABASE**

```sql
DROP DATABASE planetscale_test;
```

![DROP_DATABASE](drop-db.png)

Drop a remark based on your observation.
- [x] Works Great 💚 (This means that all the steps were executed successfuly and the expected outputs were returned.)

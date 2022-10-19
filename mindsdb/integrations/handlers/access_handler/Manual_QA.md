# Welcome to the MindsDB Manual QA Testing for Access Handler

> **Please submit your PR in the following format after the underline below `Results` section. Don't forget to add an underline after adding your changes i.e., at the end of your `Results` section.**

## Testing Access Handler with [Dataset Name](URL to the Dataset)

**1. Testing CREATE DATABASE**

```
CREATE DATABASE access_datasource       --- display name for the database
WITH ENGINE = 'access',                 --- name of the MindsDB handler
PARAMETERS = {
  "db_file": "teste.accdb"                        --- path to the database file to be used
};
```

![CREATE_DATABASE](https://ibb.co/09MGyXQ)

**2. Testing CREATE PREDICTOR**

```
CREATE PREDICTOR 
  mindsdb.home_rentals_model_access
FROM access_datasource
  (SELECT * FROM demo_data.home_rentals)
PREDICT rental_price;
```

![CREATE_PREDICTOR](https://ibb.co/ZL72Tvh)

**3. Testing SELECT FROM PREDICTOR**

```
SELECT *
FROM mindsdb.predictors
WHERE name='home_rentals_model_access';
```

![SELECT_FROM](https://ibb.co/94zf2qm)

### Results

Drop a remark based on your observation.
- [ ] Works Great 💚 (This means that all the steps were executed successfuly and the expected outputs were returned.)
- [ ] There's a Bug 🪲 [Issue Title](URL To the Issue you created) ( This means you encountered a Bug. Please open an issue with all the relevant details with the Bug Issue Template)

---
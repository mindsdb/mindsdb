# Welcome to the MindsDB Manual QA Testing for Postgres Handler

> **Please submit your PR in the following format after the underline below `Results` section. Don't forget to add an underline after adding your changes i.e., at the end of your `Results` section.**

## Testing Postgres Handler with [Dataset Name](URL to the Dataset)

**1. Testing CREATE DATABASE**

```
--- After you fill all the mandatory fields run the query with the top button or shift + enter. 
    
CREATE DATABASE example_db
WITH ENGINE = "postgres",
PARAMETERS = {
    "user": "demo_user",
    "password": "demo_password",
    "host": "3.220.66.106",
    "port": "5432",
    "database": "demo"
    };
```

![CREATE_DATABASE](https://ibb.co/wN5ZbDB)
)

**2. Testing CREATE PREDICTOR**

```
CREATE PREDICTOR 
  mindsdb.home_rentals_model
FROM example_db
  (SELECT * FROM demo_data.home_rentals)
PREDICT rental_price;
```

![CREATE_PREDICTOR](https://ibb.co/tpp2pG3)
)

**3. Testing SELECT FROM PREDICTOR**

```
SELECT *
FROM mindsdb.predictors
WHERE name='home_rentals_model';
```

![SELECT_FROM](https://ibb.co/TbZxb5w)
)

### Results

Drop a remark based on your observation.
- [X] Works Great 💚 (This means that all the steps were executed successfuly and the expected outputs were returned.)
- [ ] There's a Bug 🪲 [Issue Title](URL To the Issue you created) ( This means you encountered a Bug. Please open an issue with all the relevant details with the Bug Issue Template)

Looking forward to further testing / development.

---

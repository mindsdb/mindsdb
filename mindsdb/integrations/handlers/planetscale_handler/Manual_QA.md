# Welcome to the MindsDB Manual QA Testing for PlanetScale Handler

> **Please submit your PR in the following format after the underline below `Results` section. Don't forget to add an underline after adding your changes i.e., at the end of your `Results` section.**

## Testing PlanetScale Handler with [Car Prices Dataset](https://www.kaggle.com/code/dronax/car-prices-dataset/data)
### Results
**1. Testing CREATE DATABASE**

```
CREATE DATABASE planet_scale_db
WITH ENGINE = "planet_scale",
PARAMETERS = {
      "host":"ap-south.connect.psdb.cloud",
      "port":"3306",
      "user":"g7k984fz036t8qb62mwy",
      "password":"<password>",
      "database":"test"
    };
```

![CREATE_DATABASE](https://i.imgur.com/5Ud3gCR.png)

**2. Testing SELECT from DATABASE**

```
SELECT * FROM planet_scale_db.test.cars LIMIT 10;
```

![SELECT_DATABASE](https://i.imgur.com/R72on6B.png)

**3. Testing CREATE PREDICTOR**

```
CREATE PREDICTOR 
  mindsdb.cars_predict
FROM planet_scale_db
  (SELECT carlength,carwidth,price FROM test.cars)
PREDICT price;
```

![CREATE_PREDICTOR](https://i.imgur.com/StjrGOf.png)

**4. Testing PREDICTOR STATUS**

```
SELECT * FROM predictors;
```

![PREDICTOR_STATUS](https://i.imgur.com/D2RiAye.png)

**5. Testing SELECT FROM PREDICTOR**

```
SELECT * FROM cars_predict WHERE carlength=190 AND carwidth=60;
```

![SELECT_FROM](https://i.imgur.com/2fTDI7l.png)


Drop a remark based on your observation.
- [x] Works Great 💚 (This means that all the steps were executed successfuly and the expected outputs were returned.)
- [ ] There's a Bug 🪲 [Issue Title](URL To the Issue you created) ( This means you encountered a Bug. Please open an issue with all the relevant details with the Bug Issue Template)

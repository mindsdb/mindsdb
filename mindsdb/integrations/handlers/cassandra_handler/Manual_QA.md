# Welcome to the MindsDB Manual QA Testing for Cassandra Handler

> **Please submit your PR in the following format after the underline below `Results` section. Don't forget to add an underline after adding your changes i.e., at the end of your `Results` section.**

## Testing Cassandra Handler with [Dataset Name](URL to the Dataset)

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

## Testing Cassandra Handler with [Home Rentals](https://s3.eu-west-2.amazonaws.com/mindsdb-example-data/home_rentals.csv)

**1. Testing CREATE DATABASE**

```
CREATE DATABASE home_rentals  --- display name for database. 
WITH ENGINE = 'cassandra',     --- name of the mindsdb handler 
PARAMETERS = {
    "user": "dummy",              --- Your database user.
    "password": "1234",          --- Your password.
    "host": "1.2.3.4",              --- host, it can be an ip or an url. 
    "port": "9042",           --- common port is 9042.
    "keyspace": "home_rentals"           --- The name of your Keyspace .
};
```

![CREATE_DATABASE](https://i.ibb.co/tKpXcJS/gnome-shell-screenshot-28t8ca.png)

**2. Testing CREATE PREDICTOR**

```
CREATE PREDICTOR mindsdb.home_rentals_model
FROM home_rentals 
    (SELECT * FROM rentals)
PREDICT rental_price;
```

![CREATE_PREDICTOR](https://i.ibb.co/km17kFb/gnome-shell-screenshot-clefkk.png)

**3. Testing SELECT FROM PREDICTOR**

```
SELECT *
FROM mindsdb.predictors
WHERE name='home_rentals_model';
```

![SELECT_FROM](https://i.ibb.co/j86c8Mm/gnome-shell-screenshot-4626cp.png)

### Results

Drop a remark based on your observation.
- [x] Works Great 💚

---

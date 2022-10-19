# Welcome to the MindsDB Manual QA Testing for DynamoDB Handler

> **Please submit your PR in the following format after the underline below `Results` section. Don't forget to add an underline after adding your changes i.e., at the end of your `Results` section.**

## Testing DynamoDB Handler with [Titanic Surviver Dataset](https://www.kaggle.com/competitions/titanic/data?select=train.csv)

**1. Testing CREATE DATABASE**

```
CREATE DATABASE files.demo
WITH ENGINE = "postgres",
PARAMETERS = {
    "user": "demo_user",
    "password": "demo_password",
    "host": "3.220.66.106",
    "port": "5432",
    "database": "demo"
    };
```

![CREATE_DATABASE](https://postimg.cc/MMdQLXxT)

**2. Testing CREATE PREDICTOR**

```
CREATE PREDICTOR 
  mindsdb.home_rentals_model
FROM files.demo
  (SELECT * FROM files.demo)
PREDICT Survived;
```

![CREATE_PREDICTOR](https://postimg.cc/fkP3SGR6)

**3. Testing SELECT FROM PREDICTOR**

```
SELECT PassengerId,
        Survived 
FROM files.demo
WHERE Pclass=3
AND Sex='male'
AND Parch=0
AND Ticket='A/5 21171'
AND Fare=7.25;
```

![SELECT_FROM](https://postimg.cc/yDyYjt76)

### Results

Drop a remark based on your observation.
- [x] Works Great 💚 (This means that all the steps were executed successfuly and the expected outputs were returned.)
- [ ] There's a Bug 🪲 [Issue Title](URL To the Issue you created) ( This means you encountered a Bug. Please open an issue with all the relevant details with the Bug Issue Template)

---
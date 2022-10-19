# Welcome to the MindsDB Manual QA Testing for MySQL Handler

> **Please submit your PR in the following format after the underline below `Results` section. Don't forget to add an underline after adding your changes i.e., at the end of your `Results` section.**

## Testing MySQL Handler with [Dataset Name](URL to the Dataset)

**1. Testing CREATE DATABASE**

```
CREATE DATABASE example_db
WITH ENGINE = "mysql",
PARAMETERS = {
    "user": "demo_user",
    "password": "demo_password",
    "host": "3.220.66.106",
    "port": "5432",
    "database": "demo"
    };
```

![CREATE_DATABASE](https://ibb.co/pb5CQvG)

**2. Testing CREATE PREDICTOR**

```
CREATE PREDICTOR 
  mindsdb.home_rentals_model
FROM example_db
  (SELECT * FROM demo_data.home_rentals)
PREDICT rental_price;
```

![CREATE_PREDICTOR](https://ibb.co/p30m97k)

**3. Testing SELECT FROM PREDICTOR**

```
SELECT *
FROM mindsdb.predictors
WHERE name='home_rentals_model';
```

![SELECT_FROM](https://ibb.co/qx5mRHJ)

### Results

Drop a remark based on your observation.
- [ ] Works Great 💚 (This means that all the steps were executed successfuly and the expected outputs were returned.)
- [ ] There's a Bug 🪲 [Issue Title](URL To the Issue you created) ( This means you encountered a Bug. Please open an issue with all the relevant details with the Bug Issue Template)

---


## Testing MySQL Handler with [Predictive Maintenance](https://www.kaggle.com/datasets/tolgadincer/predictive-maintenance?select=train.csv)

**1. Testing CREATE DATABASE**

```
CREATE DATABASE predictMaintenance  
WITH ENGINE = 'mysql',       
PARAMETERS = {
    "user": "root",            
    "password": "armanchand",    
    "host": "0.tcp.in.ngrok.io",             
    "port": "15232",          
    "database": "predicitveMaintenance"          
};

```
<img width="875" alt="Screenshot 2022-10-13 at 6 37 13 PM" src="https://user-images.githubusercontent.com/26898623/195604605-586bf572-7b45-425c-8030-779958701f07.png">

**2. Testing CREATE PREDICTOR**

```
CREATE PREDICTOR mindsdb.machine_failure_rate_predicotr
FROM machine_failure                     
(SELECT * FROM machine_train LIMIT 10000)  
PREDICT Machine_failure;    
```

![je3exwn7nbdot9l1hs66](https://user-images.githubusercontent.com/26898623/195608656-26b092ab-7f4a-4bb0-81ba-1cf7d673ce86.jpg)


**3. Testing SELECT FROM PREDICTOR**

```
SELECT Machine_failure
FROM mindsdb.machine_failure_rate_predictor
WHERE torque =40;
```

![eocx8ayd6p036b6sfh6u](https://user-images.githubusercontent.com/26898623/195609435-883ce74e-021f-423b-85cf-158fc0a60b6d.jpg)


### Results

Drop a remark based on your observation.
- [X] Works Great 💚 (This means that all the steps were executed successfuly and the expected outputs were returned.)
- [ ] There's a Bug 🪲 [Issue Title](URL To the Issue you created) ( This means you encountered a Bug. Please open an issue with all the relevant details with the Bug Issue Template)

---

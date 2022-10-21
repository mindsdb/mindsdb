# Welcome to the MindsDB Manual QA Testing for CockroachDB Handler

> **Please submit your PR in the following format after the underline below `Results` section. Don't forget to add an underline after adding your changes i.e., at the end of your `Results` section.**

## Testing CockroachDB Handler with [Dataset Name](URL to the Dataset)

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

## Testing CockroachDB Handler with Rides (CockroachDB In-memory Database)
1. Testing CREATE DATABASE

```
CREATE DATABASE cockroach_demo 
WITH ENGINE = "cockroachdb",  
PARAMETERS = {
    "user": "demo",          
    "password": "demo14255", 
    "host": "0.tcp.ap.ngrok.io",     
    "port": "18953",       
    "database": "movr"
};
```

![CREATE_DATABASE](https://i.postimg.cc/9QS3t2N4/Screenshot-from-2022-10-18-19-03-08.png)


2. Testing CREATE PREDICTOR
```
CREATE PREDICTOR 
  mindsdb.rides_revenue
FROM cockroach_demo
  (SELECT * FROM public.rides)
PREDICT revenue;
```

![CREATE_PREDICTOR](https://i.postimg.cc/D0bJ5V3v/Screenshot-from-2022-10-18-19-06-08.png)


3. Testing SELECT FROM PREDICTOR
```
SELECT revenue, 
       revenue_explain 
FROM mindsdb.rides_revenue
WHERE  city = 'san francisco'
  AND vehicle_city = 'san francisco'
```

![SELECT_FROM](https://i.postimg.cc/nVQG0gk9/Screenshot-from-2022-10-18-19-07-21.png)


### Results
Works Great 💚 


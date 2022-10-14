# Welcome to the MindsDB Manual QA Testing for Clickhouse Handler

> **Please submit your PR in the following format after the underline below `Results` section. Don't forget to add an underline after adding your changes i.e., at the end of your `Results` section.**

## Testing Clickhouse Handler with [House Rent Prediction Dataset](https://www.kaggle.com/datasets/iamsouravbanerjee/house-rent-prediction-dataset)

**1. Testing CREATE DATABASE**

```
CREATE DATABASE local_clickhouse
WITH ENGINE='clickhouse',
PARAMETERS={
  "host": "10.32.36.188",
  "port": 9001,
  "database": "default",
  "user": "default",
  "password": ""
};
```
<img width="1164" alt="image" src="https://user-images.githubusercontent.com/8719716/195746882-054d63df-849d-4875-b711-c8fc01f66b7d.png">



**2. Testing CREATE PREDICTOR**

```
CREATE PREDICTOR mindsdb.home_rental_model FROM  local_clickhouse    (SELECT * FROM House_Rent_Dataset) PREDICT rent;
```

<img width="1149" alt="image" src="https://user-images.githubusercontent.com/8719716/195746592-daa11cf6-878b-49dd-a4f7-b14e2267abaf.png">

**3. Testing SELECT FROM PREDICTOR**

```
SELECT rent,rent_explain FROM mindsdb.home_rental_model
WHERE posted_on = '2022-10-10' and  bhk=2 and size1=400  and floor='Ground out of 2' and area_type='Carpet Area' and area_locality='chatana' and city='Kolkata' and furnishing_status='Unfurnished' and tenant_preferred='Bachelors/Family' and  bathroom=1 and  point_of_contact='Contact Owner';
```

<img width="1295" alt="image" src="https://user-images.githubusercontent.com/8719716/195746705-5537b2aa-37dc-41e3-bc81-fff6191ca31d.png">

### Results

Drop a remark based on your observation.
- [ ] Works Great 💚 (This means that all the steps were executed successfuly and the expected outputs were returned.)
- [ ] There's a Bug 🪲 [Issue Title](URL To the Issue you created) ( This means you encountered a Bug. Please open an issue with all the relevant details with the Bug Issue Template)

---

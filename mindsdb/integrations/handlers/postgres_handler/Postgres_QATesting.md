# Welcome to the MindsDB Manual QA Testing for Postgres Handler


## Testing Postgres Handler with [Breast Cancer Prediction](https://www.kaggle.com/datasets/shubamsumbria/breast-cancer-prediction?resource=download)

**1. Testing CREATE DATABASE**

```
--- After you fill all the mandatory fields run the query with the top button or shift + enter. 
    
CREATE DATABASE postgres_test  --- display name for database. <img width="1455" alt="Screenshot 2023-05-05 at 4 30 48 PM" src="https://user-images.githubusercontent.com/26898623/236441127-052e2a95-2442-4995-b3e2-b46717e5f73f.png">

WITH ENGINE = 'postgres',     --- name of the mindsdb handler 
PARAMETERS = {
    "user": "root",              --- Your database user.
    "password": "arman123@ABcabc",          --- Your password.
    "host": "0.tcp.in.ngrok.io",              --- host, it can be an ip or an url. 
    "port": "5432",           --- common port is 5432.
    "database": "MindsDb"           --- The name of your database *optional.
};
```
<img width="974" alt="Screenshot 2023-05-05 at 4 09 06 PM" src="https://user-images.githubusercontent.com/26898623/236440865-13f90a33-d224-46d2-82b6-36197515dcda.png">


**2. Testing CREATE PREDICTOR**

```
CREATE PREDICTOR mindsdb.cancer_predictor
FROM breastCancer (
    SELECT * FROM cancer
) PREDICT diagnosis;
```
<img width="1450" alt="Screenshot 2023-05-05 at 4 31 33 PM" src="https://user-images.githubusercontent.com/26898623/236441228-37248b36-5b87-4c35-afc8-a5476eb71715.png">


**3. Testing SELECT FROM PREDICTOR**

```
SELECT *
FROM mindsdb.models
WHERE name='cancer_predictor';
```
<img width="1451" alt="Screenshot 2023-05-05 at 4 32 25 PM" src="https://user-images.githubusercontent.com/26898623/236441396-6466ee55-92f7-4fc9-bf35-dd87501c18d7.png">



### Results

Drop a remark based on your observation.
- [X] Works Great 💚 (This means that all the steps were executed successfuly and the expected outputs were returned.)
- [ ] There's a Bug 🪲 [Issue Title](URL To the Issue you created) ( This means you encountered a Bug. Please open an issue with all the relevant details with the Bug Issue Template)

Looking forward to further testing / development.

---

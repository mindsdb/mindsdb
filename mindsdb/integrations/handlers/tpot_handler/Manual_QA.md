# Welcome to the MindsDB Manual QA Testing for TPOT Handler

> **Please submit your PR in the following format after the underline below `Results` section. Don't forget to add an underline after adding your changes i.e., at the end of your `Results` section.**

## Testing TPOT Handler with [Hospital Cost](https://github.com/mindsdb/benchmarks/blob/main/benchmarks/datasets/hospital_costs/data.csv)

**1. CREATE DATABASE**

```
Used Upload csv file feature to add Data
```

![CREATE_DATABASE](https://user-images.githubusercontent.com/75653580/221827107-9da2cb24-bd64-49e8-b45f-a271095fc51a.png)

**2. Testing CREATE PREDICTOR**

```
CREATE MODEL 
  mindsdb.hospital_tpot
FROM files
  (SELECT * FROM Hospital)
PREDICT TOTCHG
USING
engine='TPOT';
```

![CREATE_PREDICTOR]([Image URL of the screenshot](https://user-images.githubusercontent.com/75653580/221827758-83382215-67e9-45ab-a69e-b624958a5252.png))

**3. Testing SELECT FROM PREDICTOR**

```
SELECT * FROM mindsdb.hospital_tpot
WHERE AGE=19 AND 
SEX=1 AND 
LOS=2 AND 
DEMOGRAPHIC=1 AND 
APRDRG=660 ;
```

![SELECT_FROM](https://user-images.githubusercontent.com/75653580/221828074-e179d9ed-600d-46ed-9213-c9c24b1fdc03.png)

### Results

Drop a remark based on your observation.
- [x] Works Great 💚 (This means that all the steps were executed successfuly and the expected outputs were returned.)
- [] There's a Bug 🪲 [Issue Title](URL To the Issue you created) ( This means you encountered a Bug. Please open an issue with all the relevant details with the Bug Issue Template)

---
# Welcome to the MindsDB Manual QA Testing for MsSQL Handler

> **Please submit your PR in the following format after the underline below `Results` section. Don't forget to add an underline after adding your changes i.e., at the end of your `Results` section.**

## Testing MsSQL Handler with [Diabetes Data](https://raw.githubusercontent.com/mindsdb/mindsdb-examples/a43f66f0250c460c0c4a0793baa941307b09c9f2/others/diabetes_example/dataset/diabetes-train.csv)

**1. Testing CREATE DATABASE**

```
CREATE DATABASE mssql_test        --- display name for the database
WITH ENGINE='mssql',                    --- name of the MindsDB handler
PARAMETERS={
  "host": "azurehostaddress",                          --- host name or IP address
  "port": 1433,                             --- port used to make TCP/IP connection
  "database": "DIABETES_DATA",                      --- database name
  "user": "mlantz",                          --- database user
  "password": "changme"                       --- database password
};
```

![CREATE_DATABASE](https://user-images.githubusercontent.com/241893/199146358-8bb1e919-fa66-4374-98bb-10f40051488f.png)


**2. Testing CREATE PREDICTOR**

```
CREATE PREDICTOR mindsdb.mssql_diabetes_predictor
FROM mssql_test
  SELECT * FROM DIABETES_DATA.diabetes
) PREDICT class
```

![CREATE_PREDICTOR](https://user-images.githubusercontent.com/241893/199147413-ce8dc12c-313e-448e-8f66-9b9a2df8bf7a.png)

**3. Testing SELECT FROM PREDICTOR**

```
SELECT *
FROM mindsdb.predictors
WHERE name='mssql_diabetes_predictor';
```

![SELECT_FROM]!(https://user-images.githubusercontent.com/241893/199147468-798afd73-7ae6-458a-96b2-de9fbfb70bca.png)

### Results

Drop a remark based on your observation.
- [X] Works Great 💚 (This means that all the steps were executed successfuly and the expected outputs were returned.)
- [ ] There's a Bug 🪲 [Issue Title](URL To the Issue you created) ( This means you encountered a Bug. Please open an issue with all the relevant details with the Bug Issue Template)

Works great! I thought Azure databases might cause issues (works better than DBeaver which threw errors I could not fix) aside from having to grant permission for the mindsdb cloud server. Not sure if it's a big deal but the api was able to expose the mindsdb IP address. (blurred in my  ![screenshot](https://user-images.githubusercontent.com/241893/199147253-1e8aca4f-606c-48d1-8d6e-b77d821baec2.png))


---

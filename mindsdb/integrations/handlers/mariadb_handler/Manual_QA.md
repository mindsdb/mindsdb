# Welcome to the MindsDB Manual QA Testing for MariaDB Handler

> **Please submit your PR in the following format after the underline below `Results` section. Don't forget to add an underline after adding your changes i.e., at the end of your `Results` section.**

## Testing MariaDB Handler with [Home Rental]

**1. Testing CREATE DATABASE**


Default Home_Rental dataset of MindsDB

![image](https://github.com/Anuragwagh/mindsdb/assets/56196363/59e3e499-0df7-4b1d-8f49-19c98a26e753)

![output of MariaDB successfully connected to MindsDB](https://github.com/Anuragwagh/mindsdb/assets/56196363/b50b86fb-579f-4aa9-9b4d-59393ade0d26)



**2. Testing CREATE PREDICTOR**


```
CREATE MODEL [MODEL_Name]
```

**3. Testing SELECT FROM PREDICTOR**

```
FROM [MariaDB_Connection_Name] (SELECT * FROM [Dataset_Name])
PREDICT (Column_Name)
```
After Predict Query MindsDB throwing error


**4. Testing DROP DATABASE**

```
DROP DATABASE [Database_Name]
```

### Results

Drop a remark based on your observation.
- [ ] Works Great 💚 (This means that all the steps were executed successfuly and the expected outputs were returned.)
- [*] There's a Bug 🪲 [fcntl is not defined] Getting Error After PREDICT Query
---

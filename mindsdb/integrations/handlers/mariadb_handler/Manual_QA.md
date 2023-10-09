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
![Output for successfully creation of model](https://github.com/Anuragwagh/mindsdb/assets/56196363/5af86744-7dff-4771-bda4-fd43e5cd7866)


**3. Testing SELECT FROM PREDICTOR**

```
FROM [MariaDB_Connection_Name] (SELECT * FROM [Dataset_Name])
PREDICT (Column_Name)
```
![Output for successfully creation of model](https://github.com/Anuragwagh/mindsdb/assets/56196363/b315dd9e-e8ed-48e3-aef0-4c07a4e6d357)

After Predict Query MindsDB throwing error

![Web capture_9-10-2023_05735_127 0 0 1](https://github.com/Anuragwagh/mindsdb/assets/56196363/d5ea4473-2383-4bf8-a324-69729e1d52b0)

![Screenshot 2023-10-09 005244](https://github.com/Anuragwagh/mindsdb/assets/56196363/741bf150-d500-4d28-b11e-79b2f30d168c)


**4. Testing DROP DATABASE**

```
DROP DATABASE [Database_Name]
```
![Web capture_9-10-2023_05510_127 0 0 1](https://github.com/Anuragwagh/mindsdb/assets/56196363/361fa357-4c35-4d2b-b9e7-23cd39b99d99)


### Results

Drop a remark based on your observation.
- [ ] Works Great 💚 (This means that all the steps were executed successfuly and the expected outputs were returned.)
- [x] There's a Bug 🪲 [fcntl is not defined] Getting Error After PREDICT Query
---

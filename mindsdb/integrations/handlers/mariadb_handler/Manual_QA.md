# Welcome to the MindsDB Manual QA Testing for MariaDB Handler

> **Please submit your PR in the following format after the underline below `Results` section. Don't forget to add an underline after adding your changes i.e., at the end of your `Results` section.**

## Testing MariaDB Handler with [Home Rental]

**1. Testing CREATE DATABASE**

```
![image](https://github.com/Anuragwagh/mindsdb/assets/56196363/90e28362-0bdc-441e-bdfc-40458bc716fb)

![image](https://github.com/Anuragwagh/mindsdb/assets/56196363/e8a87c9c-2b89-473c-b44a-b4fc1c3b0255)

Default Home_Rental dataset of MindsDB
```

**2. Testing CREATE PREDICTOR**

```
![Output for successfully creation of model](https://github.com/Anuragwagh/mindsdb/assets/56196363/283d917e-5cc1-4eb7-94ab-66eb716f4fc5)

CREATE MODEL [MODEL_Name]

```

**3. Testing SELECT FROM PREDICTOR**

```
""FROM [MariaDB_Connection_Name] (SELECT * FROM [Dataset_Name])
PREDICT (Column_Name)

![Web capture_9-10-2023_05735_127 0 0 1](https://github.com/Anuragwagh/mindsdb/assets/56196363/f0bffde8-cbe5-4055-a44a-2bbc0497a299)

After Predict Query MindsDB throwing error

![Web capture_9-10-2023_05735_127 0 0 1](https://github.com/Anuragwagh/mindsdb/assets/56196363/caa61cdb-fe55-43d6-b1c0-695d3bae9340)

![Screenshot 2023-10-09 005244](https://github.com/Anuragwagh/mindsdb/assets/56196363/1ee19b38-3013-490c-97f5-9fd61e5435de)

```

**4. Testing DROP DATABASE**

```

'DROP DATABASE [Database_Name]'

![Web capture_9-10-2023_05510_127 0 0 1](https://github.com/Anuragwagh/mindsdb/assets/56196363/90151e94-515e-4edb-b249-4c0191ae30c4)

```

### Results

Drop a remark based on your observation.
- [ ] Works Great 💚 (This means that all the steps were executed successfuly and the expected outputs were returned.)
- [*] There's a Bug 🪲 [fcntl is not defined] Getting Error After PREDICT Query
---

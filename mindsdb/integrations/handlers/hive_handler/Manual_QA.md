# Welcome to the MindsDB Manual QA Testing for Hive Handler

## Testing Hive Handler

**1. Testing CREATE DATABASE**

```
CREATE DATABASE hive_datasource
WITH
  engine = 'hive',
  parameters = {
    "user": "hive",
    "password": "password",
    "host": "127.0.0.1",
    "port": "10000",
    "database": "default"
  };
```

[![hive-create-database.png](https://i.postimg.cc/jjdcbSYh/hive-create-database.png)](https://postimg.cc/WdCgmj3D)


**3. Testing SELECT FROM Database**

```
SELECT * FROM hive_datasource.employees;
```

[![select-from-hive.png](https://i.postimg.cc/rpXTn76c/select-from-hive.png)](https://postimg.cc/WdnxtWbf)

### Results

Drop a remark based on your observation.
- [x] Works Great 💚 (This means that all the steps were executed successfuly and the expected outputs were returned.)
- [ ] There's a Bug 🪲 [Issue Title](URL To the Issue you created) ( This means you encountered a Bug. Please open an issue with all the relevant details with the Bug Issue Template)

---
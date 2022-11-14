# Welcome to the MindsDB Manual QA Testing for Rockset Handler

> **Please submit your PR in the following format after the underline below `Results` section. Don't forget to add an underline after adding your changes i.e., at the end of your `Results` section.**

## Test 1.0
```
CREATE DATABASE rockset
WITH ENGINE = 'rockset',     
PARAMETERS = {
    "host": "127.0.0.1",
    "port": 47335,
    "user": "mindsdb",
    "database": "rockset"
};
```

**Results**

```
Error
Expected {"START" | "SET" | "USE" | "SHOW" | "DELETE" | "INSERT" | "UPDATE" | "ALTER" | "SELECT" | "ROLLBACK" | "COMMIT" | "EXPLAIN" | {"CREATE" "PREDICTOR"} | {"CREATE" "VIEW"} | "DROP" | "RETRAIN" | {"CREATE" "DATASOURCE"} | "DESCRIBE" | {"CREATE" "DATABASE"} | {"CREATE" "TABLE"} | "BEGIN"} (at char 0), (line:1, col:1)
```

![error](tests/test.png)


## Test 1.1
```
CREATE DATABASE mindsdb
WITH ENGINE = 'rockset',
PARAMETERS = {
    "host": "127.0.0.1",
    "port": 47335,
    "user": "rockset",
    "password": "password",
    "database": "mindsdb"
};
```

**Results**

```
Error: Can't connect to db: Handler 'rockset' can not be used
```

![error](tests/test2.png)

**Notes** 


**2. Testing CREATE TABLE**

```
CREATE TABLE rockset_integration.test_table (
    id INT,
    name VARCHAR(255),
    PRIMARY KEY (id)
)
```

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
- [ ] Works Great 💚 (This means that all the steps were executed successfully and the expected outputs were returned.)
- [ ] There's a Bug 🪲 [Issue Title](URL To the Issue you created) ( This means you encountered a Bug. Please open an issue with all the relevant details with the Bug Issue Template)

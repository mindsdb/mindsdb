# Welcome to the MindsDB Manual QA Testing for SurrealDB Handler

> **Please submit your PR in the following format after the underline below `Results` section.
> Don't forget to add an underline after adding your changes i.e., at the end of your `Results` section.**

## Testing SurrealDB Handler

**1. Testing CREATE DATABASE**

```
CREATE DATABASE exampledb
WITH ENGINE = 'surrealdb',
PARAMETERS = {
  "host": "6.tcp.ngrok.io",
  "port": "17141",
  "user": "root",
  "password": "root",
  "database": "testdb",
  "namespace": "testns"
};
```

![CREATE_DATABASE](https://github.com/mindsdb/mindsdb/assets/75406794/3f742332-ee22-433a-9274-8c658c19d4dd)

**2. Testing SELECT**

```
SELECT * FROM exampledb.dev;
```

![SELECT](https://github.com/mindsdb/mindsdb/assets/75406794/5fba7fb5-f0b4-4d5c-b6ac-67fe52b9c8a8)

### Results

Drop a remark based on your observation.
- [x] Works Great 💚 (This means that all the steps were executed successfully and the expected outputs were returned.)
- [ ] There's a Bug 🪲 [Issue Title](URL To the Issue you created) ( This means you encountered a Bug.
Please open an issue with all the relevant details with the Bug Issue Template)

---
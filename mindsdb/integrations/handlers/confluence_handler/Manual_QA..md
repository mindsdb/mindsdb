# Welcome to the MindsDB Manual QA Testing for Confluence Handler

## Testing Confluence Handler
**1. Testing CREATE DATABASE**

```
CREATE DATABASE mindsdb_confluence
WITH
    ENGINE = 'confluence',
    PARAMETERS = {
    "url": "https://marios.atlassian.net/",
    "username": "your_username",
    "password":"access_token" 
    };
```
[![create-database-confluence.png](https://i.postimg.cc/J41RJ3P6/create-database-confluence.png)](https://postimg.cc/14jx1Fxw)

**2. Testing SELECT FROM DATABASE**

```
SELECT * FROM mindsdb_confluence.pages;
```

[![select-from-confluence.jpg](https://i.postimg.cc/qM2fqNsJ/select-from-confluence.jpg)](https://postimg.cc/dLQNStNp)
### Results

Drop a remark based on your observation.
- [X] Works Great 💚 (This means that all the steps were executed successfuly and the expected outputs were returned.)
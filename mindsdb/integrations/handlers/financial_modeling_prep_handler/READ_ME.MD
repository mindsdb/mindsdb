# Financial Modeling Prep API Handler

This handler integrates with the [Financial Modeling Prep API](https://site.financialmodelingprep.com/developer/docs) to allow users to use ML for financial analysis and predictions.

### Connect to the Financial Modeling API
We start by creating a database to connect to the Financial Modeling API. 
Retrieve an api key from: [text](https://site.financialmodelingprep.com/developer/docs)

~~~~sql
CREATE DATABASE my_table
WITH
  ENGINE = 'FINANCIAL_MODELING'
  PARAMETERS = {
    "api_key": <api_key>,
  };
~~~~

Now, you can use this established connection to query your database as follows:
~~~~sql
SELECT * FROM my_table;

~~~~
### Select Data
```
SELECT *
FROM my_table.daily_chart_table
WHERE symbol = 'AAPL'
LIMIT 10
```

Response: 


To customize open_time(from), close_time(to): 
```
SELECT *
FROM my_table.daily_chart_table
WHERE symbol = 'AAPL';
AND from_date = '2023-10-10'
AND to_date = '2023-12-10'
```

### Connect to the Financial Modeling API
We start by creating a database to connect to the Financial Modeling API. 
Retrieve an api key from: 

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
WHERE symbol = 'AAPL';
```

Response: 

where:
* symbol 

To customize open_time(from), close_time(to): 
```
SELECT *
FROM my_table.daily_chart_table
WHERE symbol = 'AAPL';
AND open_time > '2023-10-10'
AND close_time < '2023-12-10'
```
# git push origin Riyana/FinancialHandler
# cd mindsdb/integrations/handlers/financial_modeling_prep_handler

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

# Response for AAPL Daily Chart Data

Displays closing price of the company's stock for each day in a specified date range

| Date       | Close  |
|------------|--------|
| 2023-12-08 | 195.71 |
| 2023-12-07 | 194.27 |
| 2023-12-06 | 192.32 |
| 2023-12-05 | 193.42 |
| 2023-12-04 | 189.43 |
| 2023-12-01 | 191.24 |
| 2023-11-30 | 189.95 |
| 2023-11-29 | 189.37 |
| 2023-11-28 | 190.4  |
| 2023-11-27 | 189.79 |
| ...        | ...    |
| 2023-11-20 | 191.45 |
| 2023-11-17 | 189.69 |
| 2023-11-16 | 189.71 |
| 2023-11-15 | 188.01 |
| 2023-11-14 | 187.44 |
| 2023-11-13 | 184.8  |
| 2023-11-10 | 186.4  |
| 2023-11-09 | 182.41 |
| 2023-11-08 | 182.89 |
| 2023-11-07 | 181.82 |


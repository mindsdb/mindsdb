# Financial Modeling Prep API Handler

This handler integrates with the [Financial Modeling Prep API](https://site.financialmodelingprep.com/developer/docs) to allow users to use ML for financial analysis and predictions.

### Connect to the Financial Modeling Prep API
We start by creating a database to connect to the Financial Modeling Prep API. 
Retrieve an api key from: (https://site.financialmodelingprep.com/developer/docs)

~~~~sql
CREATE DATABASE fmp_datasource
WITH
  ENGINE = 'FINANCIAL_MODELING_PREP'
  PARAMETERS = {
    "api_key": <api_key>
  };
~~~~

Now, you can use this established connection to query your database as follows:

### Select Data

~~~~sql
SELECT *
FROM fmp_datasource.historical_prices
WHERE symbol = 'AAPL'
~~~~

| Date       | Open   | High   | Low    | Close  | Adj Close | Volume   | Unadjusted Volume | Change | Change Percent | VWAP    | Change Over Time |
|------------|--------|--------|--------|--------|-----------|----------|-------------------|--------|----------------|---------|------------------|
| 2024-05-13 | 185.435| 187.1  | 184.62 | 186.93 | 186.93    | 50931607 | 50931607          | 1.5    | 0.80621242     | 186.22  | 0.0080621242     |
| 2024-05-10 | 184.9  | 185.09 | 182.13 | 183.05 | 183.05    | 50759496 | 50759496          | -1.85  | -1             | 183.7925| -0.01            |
| 2024-05-09 | 182.56 | 184.66 | 182.11 | 184.57 | 184.32    | 48982972 | 48982972          | 2.01   | 1.1            | 183.475 | 0.011            |
| ... | ... | ... | ... | ... | ...  | ... | ...      | ...  | ...     | ...| ...    |
| 2019-07-17 | 183.45 | 184.9  | 181.32 | 182.4  | 182.15    | 77305771 | 77305771          | -1.05  | -0.57236       | 183.0175| -0.0057236      |

### Use LIMIT Clause
~~~~sql
SELECT *
FROM fmp_datasource.historical_prices
WHERE symbol = 'AAPL'
LIMIT 5
~~~~

| Date       | Open   | High   | Low    | Close  | Adj Close | Volume   | Unadjusted Volume | Change | Change Percent | VWAP    | Change Over Time |
|------------|--------|--------|--------|--------|-----------|----------|-------------------|--------|----------------|---------|------------------|
| 2024-05-13 | 185.435| 187.1  | 184.62 | 186.93 | 186.93    | 50931607 | 50931607          | 1.5    | 0.80621242     | 186.22  | 0.0080621242     |
| 2024-05-10 | 184.9  | 185.09 | 182.13 | 183.05 | 183.05    | 50759496 | 50759496          | -1.85  | -1             | 183.7925| -0.01            |
| 2024-05-09 | 182.56 | 184.66 | 182.11 | 184.57 | 184.32    | 48982972 | 48982972          | 2.01   | 1.1            | 183.475 | 0.011            |
| 2024-05-08 | 182.85 | 183.07 | 181.45 | 182.74 | 182.49    | 45057087 | 45057087          | -0.11  | -0.0601586     | 182.5275| -0.000601586    |
| 2024-05-07 | 183.45 | 184.9  | 181.32 | 182.4  | 182.15    | 77305771 | 77305771          | -1.05  | -0.57236       | 183.0175| -0.0057236      |


To customize specified date range, (from_date, to_date): 
~~~~sql
SELECT *
FROM fmp_datasource.historical_prices
WHERE symbol = 'AAPL';
AND from_date = '2023-10-10'
AND to_date = '2023-12-10'
~~~~

### Response for AAPL Daily Chart Data

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
| ...        | ...    |
| 2023-11-20 | 191.45 |
| 2023-11-17 | 189.69 |
| 2023-11-16 | 189.71 |



# CoinBase API Handler

This handler integrates with the [CoinBase API](https://docs.cloud.coinbase.com/sign-in-with-coinbase/docs/api-users) to make aggregate trade data available to use for model training and predictions.

## Example: Forecast Cryptocurrency Prices

To see how the CoinBase handler is used, let's walk through a simple example to create a time series model to predict the future price of Bitcoin (BTC) in terms of USDT.

### Connect to the CoinBase API
We start by creating a database to connect to the CoinBase API.

```
CREATE DATABASE my_coinbase
WITH 
  ENGINE = 'coinbase',
  PARAMETERS = {
    "api_key": "1234",
    "api_secret": "***",
    "api_passphrase": "***"
  };
```

### Select Data
To see if the connection was successful, try searching for the most recent trade data. By default, aggregate data from the latest 1000 trading intervals with a length of 1m each are returned.

```
SELECT *
FROM my_coinbase.coinbase_candle_data
WHERE symbol = 'BTC-USD';
```

Each row should look like this:

| symbol      | low          | high        | open        | close       | volume      | timestamp   | timestamp_iso |
| ----------- | -----------  | ----------- | ----------- | ----------- | ----------- | ----------- | -----------   |
  BTC-USD       34070.49       34091.25      34088.72      34073.8       1.94718722    1698499500    2023-10-28T09:25:00-04:00
 
 where:
* symbol - Trading pair (BTC to USDT in the above example)
* low - Lowest price of base asset during trading interval
* high - Highest price of base asset during trading interval
* open - Price of base asset at beginning of trading interval
* close - Price of base asset at end of trading interval
* volume - Total amount of base asset traded during interval
* timestamp - End time of interval in seconds since the Unix epoch
* timestamp_iso - End time of interval in seconds since the Unix epoch in ISO format

You can customize symbol and interval:

```
SELECT *
FROM my_coinbase.coinbase_candle_data
WHERE symbol = 'BTC-USD'
AND interval = 300;
```

Supported intervals are [listed here](https://docs.cloud.coinbase.com/exchange/reference/exchangerestapi_getproductcandles):
* 60
* 300
* 900
* 3600
* 21600
* 86400

### Train a Model

Now it's time to create a time series model using 10000 trading intervals in the past with duration 1m.

```
CREATE MODEL mindsdb.coinbase_btc_forecast_model
FROM my_coinbase
(
  SELECT * FROM coinbase_candle_data
  WHERE symbol = 'BTC-USD'
  AND interval = 300
)

PREDICT open

ORDER BY timestamp
WINDOW 20
HORIZON 10;
```

### Making Predictions

First let's make a view for the most recent BTC-USD aggregate trade data:

```
CREATE VIEW mindsdb.recent_coinbase_data AS (
  SELECT * FROM my_coinbase.coinbase_candle_data
  WHERE symbol = 'BTC-USD'
)
```

Now let's predict the future price of BTC:

```
SELECT m.*
FROM mindsdb.recent_coinbase_data AS t
JOIN mindsdb.coinbase_btc_forecast_model AS m
WHERE m.timestamp > LATEST
```

This should give you the predicted BTC price for the next interval (we set the horizon to 10) in terms of USD.
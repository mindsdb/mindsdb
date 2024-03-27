# Kraken API Handler
This handler integrates with the [Kraken API](https://docs.kraken.com/rest/#section/General-Usage) to make data available to use for model training and predictions.

## Example: Forecast Cryptocurrency Prices
To see how the Kraken handler is used, let's walk through a simple example to create a time series model to predict the future price of Bitcoin (BTC) in terms of USDT.

```
CREATE DATABASE kraken_db
WITH 
  ENGINE = 'kraken',
  PARAMETERS = {
    "api_key": "1234",
    "api_secret": "***"
  };
```

### Select Data
To see if the connection was successful, try searching for the most recent trade data. By default, aggregate data from the latest 1000 trading intervals with a length of 1m each are returned.

### Train a Model

We can create a time series model using 10000 trading intervals in the past with duration 1m.

```
CREATE MODEL mindsdb.kraken_btc_forecast_model
FROM kraken_db
(
  SELECT * FROM kraken_trade_history
  WHERE pair = 'XXBTZUSD'
)
PREDICT price
ORDER BY time
WINDOW 20
HORIZON 10;

### Making Predictions
First let's make a view for the most recent XXBTZUSD aggregate trade data:

```
CREATE VIEW mindsdb.recent_kraken_data AS (
  SELECT * FROM kraken_db.kraken_trade_history
  WHERE pair = 'XXBTZUSD'
)
```

Now let's predict the future price of BTC:

```
SELECT m.*
FROM mindsdb.recent_kraken_data AS t
JOIN mindsdb.kraken_btc_forecast_model AS m
WHERE m.time > LATEST
```

This should give you the predicted BTC price for the next interval (we set the horizon to 10) in terms of USD.
```


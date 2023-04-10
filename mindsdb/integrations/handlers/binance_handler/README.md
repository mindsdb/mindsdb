# Binance API Handler

This handler integrates with the [Binance API](https://binance-docs.github.io/apidocs/spot/en/#change-log) to make aggregate trade (kline) data available to use for model training and predictions.

## Example: Forecast Cryptocurrency Prices

To see how the Binance handler is used, let's walk through a simple example to create a time series model to predict the future price of Bitcoin (BTC) in terms of USDT.

### Connect to the Binance API
We start by creating a database to connect to the Binance API. Currently, there is no need for an API key:

```
CREATE DATABASE my_binance
WITH
  ENGINE = 'binance'
  PARAMETERS = {};
```

### Select Data
To see if the connection was succesful, try searching for the most recent trade data. By default, aggregate data (klines) from the latest 1000 trading intervals with a length of 1m each are returned.

```
SELECT *
FROM my_binance.aggregated_trade_data
WHERE symbol = 'BTCUSDT';
```

Each row should look like this:

| symbol      | open_time    | open_price  | high_price  | low_price   | close_price | volume      | close_time    | quote_asset_volume | number_of_trades | taker_buy_base_asset_volume | taker_buy_quote_asset_volume |
| ----------- | -----------  | ----------- | ----------- | ----------- | ----------- | ----------- | -----------   | ------------------ | ---------------- | --------------------------- | ---------------------------- |
| BTCUSDT     | 1678338600| 21752.65000 | 21761.33000 | 21751.53000 | 21756.7000  | 103.8614100 | 1678338659.999 | 2259656.20520700   | 3655             | 55.25763000                 | 1202219.60971860

where:
* symbol - Trading pair (BTC to USDT in the above example)
* open_time - Start time of interval in seconds since the Unix epoch (default interval is 1m)
* open_price - Price of base asset at beginning of trading interval
* high_price - Highest price of base asset during trading interval
* low_price - Lowest price of base asset during trading interval
* close_price - Price of base asset at end of trading interval
* volume - Total amount of base asset traded during interval
* close_time - End time of interval in seconds since the Unix epoch
* quote_asset_volume - Total amount of quote asset (USDT in above case) traded during interval
* number_of_trades - Total number of trades made during interval
* taker_buy_base_asset_volume - How much of the base asset volume is contributed by taker buy orders
* taker_buy_quote_asset_volume - How much of the quote asset volume is contributed by taker buy orders


You can customize open_time, close_time, and interval:

```
SELECT *
FROM my_binance.aggregated_trade_data
WHERE symbol = 'BTCUSDT'
AND open_time > '2023-01-01'
AND close_time < '2023-01-03 08:00:00'
AND interval = '1s'
LIMIT 10000;
```

Supported intervals are [listed here](https://binance-docs.github.io/apidocs/spot/en/#kline-candlestick-data):
* 1s
* 1m
* 3m
* 5m
* 15m
* 30m
* 1h
* 2h
* 4h
* 6h
* 8h
* 12h
* 1d
* 3d
* 1w

### Train a Model

Now it's time to create a time series model using 10000 trading intervals in the past with duration 1m.

```
CREATE MODEL mindsdb.btc_forecast_model
FROM my_binance
(
  SELECT * FROM aggregated_trade_data
  WHERE symbol = 'BTCUSDT'
  AND close_time < '2023-01-01'
  AND interval = '1m'
  LIMIT 10000;
)

PREDICT open_price

ORDER BY open_time
WINDOW 100
HORIZON 10;
```

It may take a few minutes to complete. For more accuracy, you should increase the limit to be higher (e.g. 100,000)

### Making Predictions

First let's make a view for the most recent BTCUSDT aggregate trade data:

```
CREATE VIEW recent_btcusdt_data AS (
  SELECT * FROM my_binance.aggregated_trade_data
  WHERE symbol = 'BTCUSDT'
)
```

Now let's predict the future price of BTC:

```
SELECT m.*
FROM recent_btcusdt_data AS t
JOIN mindsdb.btc_forecast_model AS m
WHERE m.open_time > LATEST
```

This should give you the predicted BTC price for the next 10 minutes (we set the horizon to 10) in terms of USDT.
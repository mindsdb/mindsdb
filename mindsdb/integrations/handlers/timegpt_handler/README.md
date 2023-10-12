# Briefly describe what ML framework does this handler integrate to MindsDB, and how?
TimeGPT is a zero-shot forecasting model developed by Nixtla, offered through their `nixtlats` Python package.

This handler provides a simple wrapper around TimeGPT, and provides easy ingestion of time series data from other MindsDB data sources into the TimeGPT API. User requires a Nixtla API key to use this handler.

Call this handler by `USING ENGINE="timegpt"`.

# Why is this integration useful? What does the ideal predictive use case for this integration look like? When would you definitely not use this integration?
Zero-shot forecasting models can produce forecasts for novel data without a previous training phase a pre-requisite. However, these models can be finetuned for improved accuracy on any specific domain.

# Are models created with this integration fast and scalable, in general?
Yes.

# What are the recommended system specifications for models created with this framework?
N/A - no model training or inference is done on premise.

# To what degree can users control the underlying framework by passing parameters via the USING syntax?
Usual time series related arguments can be passed (forecast "horizon", "window", and what columns to order and group by).

Frequency of the series can be manually set, although by default it is automatically inferred from the data.

The data frequency can be specified with the "frequency" arg. If no frequency is specified, MindsDB tries to infer this automatically from the dataframe.

The confidence level for the prediction intervals can be specified with the "level" argument. The default value is `90`.

# Does this integration offer model explainability or insights via the DESCRIBE syntax?
Minimal, but yes.

# Does this integration support fine-tuning pre-existing models (i.e. is the update() method implemented)? Are there any caveats?
Not yet. This is a planned feature.

# Are there any other noteworthy aspects to this handler?
No.

# Any directions for future work in subsequent versions of the handler?
Mostly achieving full coverage of all options provided by the TimeGPT API.

# Please provide a minimal SQL example that uses this ML engine (pointers to integration tests in the PR also valid)
```sql
CREATE DATABASE my_binance
WITH
  ENGINE = 'binance'
  PARAMETERS = {};

CREATE VIEW binance_view (
  SELECT symbol, open_time, open_price
  FROM my_binance.aggregated_trade_data
  WHERE symbol = 'BTCUSDT'
);

CREATE ML_ENGINE timegpt FROM timegpt;

CREATE MODEL mindsdb.timegpt_binance
FROM mindsdb
  (SELECT symbol, open_time, open_price FROM binance_view)
PREDICT open_price
ORDER BY open_time
GROUP BY symbol
WINDOW 160
HORIZON 15
USING engine = 'timegpt';

SELECT m.symbol, m.open_time, m.open_price, m.confidence, m.lower, m.upper
FROM binance_view as t
JOIN
timegpt_binance as m
WHERE
t.open_time > LATEST;
```
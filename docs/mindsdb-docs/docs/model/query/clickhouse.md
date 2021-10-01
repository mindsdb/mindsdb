# Query the model from ClickHouse database

This section assumes that you have trained a new model using the [ClickHouse client](/model/clickhouse/) or [MindsDB Studio](/model/train/). To query the model, you will need to `SELECT` from the model table:

```sql
SELECT
  <target_variable> AS predicted,
  <target_variable_confidence> AS confidence,
  <target_variable_explain> AS info
FROM
  mindsdb.<model_name>
WHERE
  <feature_one> AND <feature_two>
```

!!! question "Query the model from other databases"
    Note that even if you have trained the model from the ClickHouse database, you will be able to query it from other databases too.

!!! warning "Using functions inside WHERE clause"
    There is an issue where ClickHouse is not parsing the functions sent inside the WHERE clause. So, for e.g toDate() or toDateTime() inside WHERE will not work. For now you can avoid using the functions until we got feedback from ClickHouse. Track the progress of this issue [here](https://github.com/ClickHouse/ClickHouse/issues/24093).

## Query example

The following example shows you how to train a new model from the ClickHouse client. The table used for training the model is the [Air Pollution in Seoul](https://www.kaggle.com/bappekim/air-pollution-in-seoul) timeseries dataset. MindsDB will predict the `SO2` (Sulfur dioxide) value in the air based on the values added in the `WHERE` statement.

```sql
SELECT
   SO2 AS predicted,
   SO2_confidence AS confidence,
   SO2_explain AS info
FROM airq_predictor
WHERE (NO2 = 0.005) AND (CO = 1.2) AND (PM10 = 5)
```
You should get a response from MindsDB similar toas:

| SO2  | confidence | info   |
|----------------|------------|------|
| 0.009897379182791115 | 0.99 | Check JSON below  |

```json
info:{
   "predicted_value": 0.009897379182791113,
   "confidence": 0.99,
   "prediction_quality": "very confident",
   "confidence_interval": [0.0059810733322441575, 0.01381368503333807], "important_missing_information": ["Address"]
}
```

![Model predictions](/assets/predictors/clickhouse-query.gif)



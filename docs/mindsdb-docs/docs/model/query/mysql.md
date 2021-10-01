# Query the model from MySQL database

This section assumes that you have trained a new model using [MySQL](/model/mysql/) or [MindsDB Studio](/model/train/).

!!! info "Prerequisite"
    Don't forget to enable FEDERATED Storage Engine as explained in [connect your data section](/datasources/mysql/#prerequisite).

To query the model, you will need to `SELECT` from the model table:

```sql
SELECT
  <target_variable> AS predicted,
  <target_variable_confidence> AS confidence,
  <target_variable_explain> AS info
FROM
  mindsdb.<model_name>
WHERE
  when_data=<JSON features values>
```
!!! question "Query the model from other databases"
    Note that even if you have trained the model from the MySQL database, you will be able to
    query it from other databases too.

## Query example

The following example shows you how to query the model from a MySQL client. The table used for training the model is the  [Us consumption](https://github.com/robjhyndman/fpp2-package/blob/15916e4fe827d1b3dcf82785a4ace80107af5ddd/data-raw/usconsumption.csv) dataset. MindsDB will predict the `consumption` based on the values added in `when_data`.

```sql
SELECT
  consumption AS predicted,
  consumption_confidence AS confidence,
  consumption_explain AS info
FROM
  mindsdb.us_consumption
WHERE
  when_data='{"income": 1.182497938, "production": 5.854555956,"savings": 3.183292657, "unemployment": 0.1, "t":"2020-01-02"}';
```
You should get a response from MindsDB similar to:

| consumption  | confidence | info   |
|----------------|------------|------|
| 1.0 | 0.93 | Check JSON below  |

```json
info: {
   "predicted_value": "1.0",
   "confidence": 0.93,
   "prediction_quality": "very confident",
   "important_missing_information": []
}
```

![Model predictions](/assets/predictors/mysql-query.gif)

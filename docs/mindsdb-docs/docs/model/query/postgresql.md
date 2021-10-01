# Query the model from PostgreSQL database

This section assumes that you have trained a new model using [psql](/model/postgresql/) or [MindsDB Studio](/model/train/). 


!!! info "Prerequisite"
    Don't forget to install the MySQL foreign data wrapper as explained in [connect your data section](/datasources/postgresql/#prerequisite).

To query the model, you will need to `SELECT` from the model table:

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
    Note that even if you have trained the model from the PostgreSQL database, you will be able to query it from other databases too.

## Query example

The following example shows you how to query the model from a psql client. The table used for training the model is the [Airline Passenger sattisfaction](https://www.kaggle.com/teejmahal20/airline-passenger-satisfaction) dataset. MindsDB will predict the `satisfaction`  based on the values added in the `WHERE` statement.

```sql
SELECT satisfaction AS predicted,
      satisfaction_confidence AS confidence,
      satisfaction_explain AS info
FROM mindsdb.airline_survey_model
WHERE "Customer Type"='Loyal Customer'
 AND age=52
 AND "Type of Travel"='Business travel'
 AND "Class"='Eco';
```
You should get a response from MindsDB similar to:

| satisfaction  | confidence | info   |
|----------------|------------|------|
| satisfied | 0.94 | Check JSON below  |

```json
info: {
 "predicted_value": "satisfied",
 "confidence": 0.94,
 "prediction_quality": "very confident",
 "important_missing_information": [
   "id",
   "Inflight wifi service",
   "Online boarding",
   "Seat comfort",
   "Baggage handling"
 ]
}
```

![Model predictions](/assets/predictors/postgresql-query.gif)



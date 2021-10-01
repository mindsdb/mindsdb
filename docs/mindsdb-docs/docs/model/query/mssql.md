# Query the model from Microsoft SQL Server

This section assumes that you have trained a new model using [SQL client](/model/mssql/) or [MindsDB Studio](/model/train/). 

!!! info "Prerequisite"
    Don't forget to install the prerequisites as explained in [connect your data section](/datasources/mssql/#prerequisite).

To query the model, you will need to `SELECT` from the model table:

```sql
exec('SELECT <target_variable> AS predicted,
             <target_variable_confidence> AS confidence,
             <target_variable_explain> AS info 
      FROM mindsdb.<model_name>
      WHERE <feature value>') AT mindsdb;
```

!!! question "Query the model from other databases"
    Note that even if you have trained the model from the Microsoft SQL Server, you will be able to
    query it from other databases too.

## Query example

The following example shows you how to query the model from a mssql client. The table used for training the model is the [Medical insurance](https://www.kaggle.com/mirichoi0218/insurance) dataset. MindsDB will predict the `charges` based on the values added in `when_data`.

```sql
exec('SELECT charges AS predicted,
             charges_confidence AS confidence,
             charges_explain AS info 
      FROM mindsdb.insurance_model
      WHERE age=30') AT mindsdb;
```
You should get a response from MindsDB similar to:

| predicted  | confidence | info   |
|----------------|------------|------|
| 7571.147 | 0.9 | Check JSON below  |

```json
info: {
  "predicted_value": 7571.147932782108, 
  "confidence": 0.9, 
  "prediction_quality": "very confident", 
  "important_missing_information": ["smoker", "bmi"]
```

![Model predictions](/assets/predictors/mssql-query.gif)

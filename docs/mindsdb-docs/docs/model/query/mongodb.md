# Query the model from MongoDB API

This section assumes that you have trained a new model using [MongoDB client](/model/mongodb/) or [MindsDB Studio](/model/train/).

To get the predictions from the model, you will need to call `find()` method on the model collection and provide values for which you want to get prediction as an object:

```sql
db.model_name.find({'key': 'value', 'key':'value'})
```

!!! Info "Note"
    The object provided to `find()` method must be valid JSON format.

## Query example

The following example shows you how to query the model from a mongo client. The collection used for training the model is the [Telcom Customer Churn](https://www.kaggle.com/blastchar/telco-customer-churn) dataset.  MindsDB will predict the customer `Churn` value based on the object values sent to `find()` method.

```sql
db.churn.find({'PhoneService': 'Yes','InternetService': 'DSL', 'OnlineService': 'No','MonthlyCharges': 53.85, 'TotalCharges': 108.15, 'tenure': 2, 'PaperlessBilling': 'Yes'})
```
You should get a response from MindsDB similar to:

| predicted_value  | confidence | info   |
|----------------|------------|------|
| Yes | 0.8 | Check JSON below  |

```json
{
  "Churn": "Yes",
  "Churn_confidence": 0.8,
  "Churn_explain": {
    "class_distribution": {
      "No": 0.44513007027299717,
      "Yes": 0.5548699297270028
    },
    "predicted_value": "Yes",
    "confidence": 0.8,
    "prediction_quality": "very confident",
    "important_missing_information": [
      "Contract"
    ]
  }
}
```

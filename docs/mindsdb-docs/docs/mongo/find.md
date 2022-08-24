# Making Predictions using the ML Models

## Using the `find({})` Method

### Description

The `find({})` method is used to get predictions from the model table. The data is not persistent and returned on the fly as a result-document.

### Syntax

```sql
db.name_of_your_predictor.find({<column>:<value>,<column>:<value>});
```

On execution, we get:

```json
    "column_name1" : value,
    "column_name2": value,
    ...columns
    "select_data_query": null,
    "when_data": null,
    "<target_variable>_original": value,
    "<target_variable>_confidence": value,
    "<target_variable>_explain": "{\"predicted_value\": value, \"confidence\": value, \"anomaly\": null, \"truth\": null, \"confidence_lower_bound\": value \"confidence_upper_bound\": value}",
    "<target_variable>_anomaly": value,
    "<target_variable>_min": value,
    "<target_variable>_max": value
```

Where:

| Expressions                        | Description                                                                                                                      |
| ---------------------------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| `<target_variable>_original`       | The real value of the target variable from the collection                                                                        |
| `<target_variable>_confidence`     | Model confidence                                                                                                                 |
| `<target_variable>_explain`        | JSON object that contains additional information, such as `confidence_lower_bound`, `confidence_upper_bound`, `anomaly`, `truth` |
| `<[target_variable>_anomaly`       | Model anomaly                                                                                                                    |
| `<[target_variable>_min`           | Lower bound value                                                                                                                |
| `<[target_variable>_max`           | Upper bound value                                                                                                                |

## Example

### Making a Single Prediction

The following SQL statement fetches the predicted value of the `rental_price` column from the `home_rentals_model` model. The predicted value is the rental price of a property with attributes listed as a parameter to the `find({})` method.

```sql
db.home_rentals_model.find({'sqft':823, 'location': 'good','neighborhood':'downtown', 'days_on_market': 10});
```

On execution, we get:

```json
{
    "sqft": 823,
    "location": "good",
    "neighborhood": "downtown",
    "days_on_market": 10,
    "number_of_rooms": null,
    "number_of_bathrooms": null,
    "initial_price": null,
    "rental_price": 1431.323795180614,
    "select_data_query": null,
    "when_data": null,
    "rental_price_original": null,
    "rental_price_confidence": 0.99,
    "rental_price_explain": "{\"predicted_value\": 1431.323795180614, \"confidence\": 0.99, \"anomaly\": null, \"truth\": null, \"confidence_lower_bound\": 1379.4387560440227, \"confidence_upper_bound\": 1483.2088343172054}",
    "rental_price_anomaly": null,
    "rental_price_min": 1379.4387560440227,
    "rental_price_max": 1483.2088343172054
}
```

### Making Bulk Predictions

!!! warning "Bulk Predictions WIP"
    The bulk predictions is a work in progress.

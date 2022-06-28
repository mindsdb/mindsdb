# `#!sql SELECT` statement

## Description

The `#!sql SELECT` statement is used to get a predictions from the model table. The data is not persistent and is returned on the fly as a result-set. The basic syntax for selecting from the model is:

## Syntax

```sql
SELECT [target_variable], [target_variable]_explain
FROM mindsdb.[predictor_name]
WHERE [column]=[value] 
    AND [column]=[value];
```

!!! warning "Grammar matters"
Ensure that there are no spaces between the column name, equal sign and value. Ensure to not use any quotations for numerical values and singular quotes for strings

On execution, you should get:

```sql
+----------+----------+-------------------+-----------------------------------------------------------------------------------------------------------------------------------------------+
| [column] | [column] | [target_variable] | [target_variable]_explain                                                                                                                     |
+----------+----------+-------------------+-----------------------------------------------------------------------------------------------------------------------------------------------+
| [value]  | [value]  | [predicted_value] | {"predicted_value": 4394, "confidence": 0.99, "anomaly": null, "truth": null, "confidence_lower_bound": 4313, "confidence_upper_bound": 4475} |
+----------+----------+-------------------+-----------------------------------------------------------------------------------------------------------------------------------------------+
```

Where:

| Expressions                              | Description                                                                                                                 |
| ---------------------------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| `[target_variable]`                      | Name of the column to be predicted                                                                                          |
| `[target_variable]`\_explain             | JSON object that contains additional information as `confidence_lower_bound`, `confidence_upper_bound`, `anomaly`, `truth`. |
| `[predictor_name]`                       | Name of the model to be used to make the prediction                                                                         |
| `#!sql WHERE [column]=[value] AND ...`   | `#!sql WHERE` clause used to pass the input data to make the prediction                                                     |

## Example

The following SQL statement selects a `rental_price` prediction from the `home_rentals_model` for a property that has the attributes named after the `#!sql WHERE` expression:

```sql
SELECT location, neighborhood, days_on_market, rental_price, rental_price_explain
FROM mindsdb.home_rentals_model1
    WHERE sqft = 823
    AND location='good'
    AND neighborhood='downtown'
    AND days_on_market=10;
```

On execution,

```sql
+----------+--------------+----------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------+
| location | neighborhood | days_on_market | rental_price | rental_price_explain                                                                                                                          |
+----------+--------------+----------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------+
| good     | downtown     | 10             | 4394         | {"predicted_value": 4394, "confidence": 0.99, "anomaly": null, "truth": null, "confidence_lower_bound": 4313, "confidence_upper_bound": 4475} |
+----------+--------------+----------------+--------------+-----------------------------------------------------------------------------------------------------------------------------------------------+
```

!!! tip "Bulk predictions"
    You can also make bulk predictions by joining a table with your model:

    ```sql
    SELECT t.rental_price as real_price,
        m.rental_price as predicted_price,
        t.sqft, t.location, t.days_on_market
    FROM example_db.demo_data.home_rentals as t
    JOIN mindsdb.home_rentals_model as m limit 100
    ```

# SELECT statement

The `SELECT` statement is used to get a predictions from the model table. The data is not persistent and is returned on the fly as a result-set. The basic syntax for selecting from the model is:

```sql
SELECT target_variable, target_variable_explain FROM model_table 
                                                WHERE column3= "value" AND  column2= "value";
```

## Model table columns 

The below list contains the column names of the model table. Note that `target_variable_` will be the name of the target variable column.

* target_variable_original - The original value of the target variable.
* target_variable_min - Lower bound of the predicted value.
* target_variable_max - Upper bound of the predicted value.
* target_variable_confidence - Model confidence score.
* target_variable_explain - JSON object that contains additional information as `confidence_lower_bound`, `confidence_upper_bound`, `anomaly`, `truth`.
* select_data_query - SQL select query to create the datasource.

{{ read_csv('https://raw.githubusercontent.com/mindsdb/mindsdb-examples/master/classics/home_rentals/home_rentals_model.csv') }}

## SELECT example

The following SQL statement selects all information from the `home_rentals_model` for the property that has "sqft": 800, "number_of_rooms": 4, "number_of_bathrooms": 2,
"location": "good", "days_on_market" : 12, "neighborhood": "downtown", "initial_price": "2222".


```sql
SELECT * FROM mindsdb.home_rentals_model 
WHERE when_data='{"sqft": 800, "number_of_rooms": 4, "number_of_bathrooms": 2,
				  "location": "good", "days_on_market" : 12, 
                  "neighborhood": "downtown", "initial_price": "2222"}';

```

![SELECT model_name](/assets/sql/select_hr.png)


The following SQL statement selects only the target variable `rental_price` as `price` and the `home_rentals_model` confidence as `accuracy`:


```sql
SELECT rental_price as price, 
rental_price_confidence as confidence 
FROM mindsdb.home_rentals_model WHERE when_data='{"sqft": 800, "number_of_rooms": 4, "number_of_bathrooms": 2, 
                                            "location": "good", "days_on_market" : 12,  
                                            "neighborhood": "downtown", "initial_price": "2222"}';
```

![SELECT model_name](/assets/sql/select_hra.png)

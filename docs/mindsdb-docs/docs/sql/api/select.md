# SELECT statement

The `SELECT` statement is used to get a predictions from the model table. The data is not persistent and is returned on the fly as a result-set. The basic syntax for selecting from the model is:

```sql
SELECT target_variable, target_variable_explain FROM mindsdb.model_table 
                                                WHERE column3=value AND  column2='value';
```

>*Side Note:*
>*Ensure that there are no spaces between the column name,equal sign and value.*
>*Ensure to not use any quotations for numerical values and singular quotes for strings.*

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

The following SQL statement selects all information from the `home_rentals` for the property that has the following attributes:
- sqft: 800
- number_of_rooms: 4
- number_of_bathrooms: 2
- location: good
- days_on_market: 12
- neighborhood: downtown
- initial_price: 2222


```sql
SELECT * FROM home_rentals WHERE sqft=800 AND number_of_rooms=4 AND number_of_bathrooms=2 AND location='good' AND days_on_market=12 AND neighborhood='downtown' AND initial_price=2222;

```

Results:

```bash
+------+-----------------+---------------------+----------+----------------+--------------+---------------+--------------------+-------------------+-----------+-----------------------+-------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+-------------------+--------------------+
| sqft | number_of_rooms | number_of_bathrooms | location | days_on_market | neighborhood | initial_price | rental_price       | select_data_query | when_data | rental_price_original | rental_price_confidence | rental_price_explain                                                                                                                                                                   | rental_price_anomaly | rental_price_min  | rental_price_max   |
+------+-----------------+---------------------+----------+----------------+--------------+---------------+--------------------+-------------------+-----------+-----------------------+-------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+-------------------+--------------------+
|  800 | 4               | 2                   | good     |             12 | downtown     |          2222 | 2263.4233195080546 | NULL              | NULL      | NULL                  | 0.99                    | {"predicted_value": 2263.4233195080546, "confidence": 0.99, "anomaly": null, "truth": null, "confidence_lower_bound": 2167.415674883504, "confidence_upper_bound": 2359.4309641326054} | NULL                 | 2167.415674883504 | 2359.4309641326054 |
+------+-----------------+---------------------+----------+----------------+--------------+---------------+--------------------+-------------------+-----------+-----------------------+-------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------+-------------------+--------------------+
1 row in set (0.71 sec)
```


The following SQL statement selects only the target variable `rental_price` and the `home_rentals` confidence:


```sql
SELECT rental_price, rental_price_confidence FROM home_rentals WHERE sqft=800 AND number_of_rooms=4 AND number_of_bathrooms=2 AND location='good' AND days_on_market=12 AND neighborhood='downtown' AND initial_price=2222;
```

Results:

```bash
mysql> SELECT rental_price, rental_price_confidence FROM home_rentals WHERE sqft=800 AND number_of_rooms=4 AND number_of_bathrooms=2 AND location='good' AND days_on_market=12 AND neighborhood='downtown' AND initial_price=2222;
+--------------------+-------------------------+
| rental_price       | rental_price_confidence |
+--------------------+-------------------------+
| 2263.4233195080546 | 0.99                    |
+--------------------+-------------------------+
1 row in set (0.69 sec)
```

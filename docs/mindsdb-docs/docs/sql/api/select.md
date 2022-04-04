# SELECT statement

The `SELECT` statement is used to get a predictions from the model table. The data is not persistent and is returned on the fly as a result-set. The basic syntax for selecting from the model is:

```sql
SELECT target_variable, target_variable_explain FROM mindsdb.model_table 
                                                WHERE column3=value AND  column2=value;
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

The following SQL statement selects all information from the `home_rentals_model` for the property that has the following attributes:

- sqft: 823
- location: good
- days_on_market: 10
- neighborhood: downtown


```sql
SELECT rental_price, rental_price_explain FROM mindsdb.home_rentals_model
WHERE sqft = 823 AND location='good' AND neighborhood='downtown' AND days_on_market=10;
```

![SELECT from model](/assets/sql/select.png)

You can also make bulk predictions by joining a table with your model:

```sql
SELECT t.rental_price as real_price, 
       m.rental_price as predicted_price,
       t.sqft, t.location, t.days_on_market 
FROM example_db.demo_data.home_rentals as t 
JOIN mindsdb.home_rentals_model as m limit 100
```

![SELECT from model bulk](/assets/sql/select_bulk.png)

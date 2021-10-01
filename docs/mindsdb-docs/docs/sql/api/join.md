# JOIN clause

The `JOIN` clause is used to combine rows from the database table and the model table on a related column. The basic syntax for joining from the data table and model is:

```sql
SELECT t.column_name1, t.column_name2, FROM integration_name.table AS t 
                                       JOIN mindsdb.predictor_name AS p WHERE t.column_name IN (value1, value2, ...);
```

## JOIN example

The following SQL statement joins the `home_rentals` data with the `home_rentals_model` predicted price:

```sql
SELECT * FROM db_integration.house_rentals_data AS t JOIN mindsdb.home_rentals_model AS tb 
                                                       WHERE t.neighborhood in ('downtown', 'south_side');
```

{{ read_csv('https://raw.githubusercontent.com/mindsdb/mindsdb-examples/master/classics/home_rentals/home_rentals_model1.csv') }}

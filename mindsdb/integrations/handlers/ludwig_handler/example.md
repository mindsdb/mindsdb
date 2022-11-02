# Preliminaries

1. Make sure your python environment has mindsdb and ludwig installed.

# MindsDB example commands

We will follow the "home rentals" tutorial available on the cloud learning hub:

1. Select a subsample of the data to inspect it:

```sql
SELECT * 
FROM example_db.demo_data.home_rentals 
LIMIT 10;
```

2. Create an AI Table with Ludwig as the ML backend:
   
```sql
CREATE MODEL 
  mindsdb.home_rentals_ludwig_model
FROM example_db
  (SELECT * FROM demo_data.home_rentals)
PREDICT rental_price
USING
engine='ludwig';
```

3. Check the status of the predictor. It may take a while to finish training:

```sql
SELECT * FROM mindsdb.models where name='home_rentals_ludwig_model';
```

3. Make a prediction. Note that for the time being Ludwig requires all input columns to be specified, an error will trigger if data for any of them is missing:
   
```sql 
SELECT rental_price
FROM mindsdb.home_rentals_ludwig_model
WHERE sqft = 823
AND number_of_rooms=2
AND number_of_bathrooms=1
AND location='good'
AND neighborhood='downtown'
AND days_on_market=10;
```
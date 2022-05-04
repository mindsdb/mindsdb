# CREATE VIEW statement

In MindsDB, an `AI Table` is a virtual table based on the result-set of the SQL Statement that `JOINS` table data with the predictions of a model. An `AI Table` can be created using the `CREATE VIEW mindsdb.ai_table_name` statement.


## Syntax
```sql
CREATE VIEW mindsdb.ai_table_name as (
    SELECT
        a.column_name,
        a.column_name2,
        a.column_name3,
        p.model_column as model_column
    FROM integration_name.table_name as a
    JOIN mindsdb.predictor_name as p
);
```


## Example

We will use the Home Rentals dataset to create an AI Table.

{{ read_csv('https://raw.githubusercontent.com/mindsdb/mindsdb-examples/master/classics/home_rentals/dataset/train.csv', nrows=2) }}

The first step is to execute a SQL query for creating a `home_rentals_model` that learns to predict the `rental_price` value given other features of a real estate listing:

```sql
CREATE PREDICTOR mindsdb.home_rentals_model
FROM integration_name (SELECT * FROM house_rentals_data) as rentals
PREDICT rental_price as price;
```

Once trained, we can `JOIN` any input data with the trained model and store the results as an AI Table. 

Let's pass some of the expected input columns (in this case, `sqft`, `number_of_bathrooms`, `location`) to the model and join the predicted `rental_price` values:

```sql
CREATE VIEW mindsdb.home_rentals as (
    SELECT
        a.sqft,
        a.number_of_bathrooms,
        a.location,
        p.rental_price as price
    FROM mysql_db.home_rentals as a
    JOIN mindsdb.home_rentals_model as p 
);
```

Note that in this example, we pass part of the same data that was used to train as a test query, but usually you would create an AI table to store predictions for new data. 
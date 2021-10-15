# CREATE VIEW statement

!!! info "Work in progress" Note this feature is in beta version. If you have additional questions or issues [reach out to us on Slack](https://join.slack.com/t/mindsdbcommunity/shared_invite/zt-o8mrmx3l-5ai~5H66s6wlxFfBMVI6wQ).

In MindsDB, the `AI Table` is a virtual table based on the result-set of the SQL Statement that `JOINS` the table data with the models prediction. The `AI Table` can be created using the `CREATE AI table ai_table_name` statement.

```sql
CREATE VIEW ai_table_name as (
    SELECT
        a.colum_name,
        a.colum_name2,
        a.colum_name3,
        p.model_column as model_column
    FROM integration_name.table_name as a
    JOIN predictor_name as p
);
```

## Example view

The below table can be `JOINED` with the model trained from it as an AI Table.

{{ read_csv('https://raw.githubusercontent.com/mindsdb/mindsdb-examples/master/classics/home_rentals/dataset/train.csv', nrows=2) }}

SQL Query for creating the home_rentals_model that predicts rental_price:

```sql
CREATE PREDICTOR home_rentals_model
FROM integration_name (SELECT * FROM house_rentals_data) as rentals
PREDICT rental_price as price;
```

Join the predicted `rental_price` from the model with the `sqft`, `number_of_bathrooms`, `location` from the table:

```sql
CREATE AI table home_rentals as (
    SELECT
        a.sqft,
        a.number_of_bathrooms,
        a.location,
        p.rental_price as price
    FROM mysql_db.home_rentals as a
    JOIN home_rentals_model as p
);
```

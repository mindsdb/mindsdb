# `#!sql CREATE VIEW` Statement

## Description

In MindsDB, an **AI Table** is a virtual table based on the result-set of the SQL Statement that `#!sql JOIN`s a table data with the predictions of a model. An **AI Table** can be created using the `#!sql CREATE VIEW` statement.

## Syntax

```sql
CREATE VIEW mindsdb.[ai_table_name] as (
    SELECT
        a.[column_name1],
        a.[column_name2],
        a.[column_name3],
        p.[model_column] as model_column
    FROM [integration_name].[table_name] as a
    JOIN mindsdb.[predictor_name] as p
);

```

Where:

| Expressions                         | Description                                                                   |
| ----------------------------------- | ----------------------------------------------------------------------------- |
| `[ai_table_name]`                   | Name of the view to be created                                                |
| `[column_name1], [column_name2] ...` | Name of the columns to be joined, input for the model to make a prediction   |
| `[model_column]`                    | name of the target column for the predictions                                 |
| `[integration_name].[table_name]`   | where `integration_name` is the linked database and has it's `table_name`     |
| `[predictor_name]`                  | Name of the model to be used to generate the predictions                      |

## Example

Having executed a SQL query for training a `home_rentals_model` that learns to predict the `rental_price` value given other features of a real estate listing:

```sql
CREATE PREDICTOR mindsdb.home_rentals_model
FROM integration_name
    (SELECT * FROM house_rentals_data) as rentals
PREDICT rental_price as price;
```

Once trained, we can [`#!sql JOIN`](/sql/api/join/) any input data with the trained model and store the results as an AI Table using the `#!sql CREATE VIEW` syntax.

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

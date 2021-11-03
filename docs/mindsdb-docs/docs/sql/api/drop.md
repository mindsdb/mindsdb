# DROP statement

!!! info "Work in progress"
    Note this feature is in beta version. If you have additional questions or issues [reach out to us on Slack](https://join.slack.com/t/mindsdbcommunity/shared_invite/zt-o8mrmx3l-5ai~5H66s6wlxFfBMVI6wQ).
    
The `DROP` statement is used to delete an existing model table.

## DROP TABLE statement

The `DROP PREDICTOR` statement is used to delete the model table:

```sql
DROP PREDICTOR table_name;
```

### DROP TABLE example

The following SQL statement drops the model table called `home_rentals_model`:

```sql
DROP PREDICTOR home_rentals_model;
```

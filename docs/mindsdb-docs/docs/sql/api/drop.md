# DROP statement

!!! info "Work in progress"
    Note this feature is in beta version. If you have additional questions or issues [reach out to us on Slack](https://join.slack.com/t/mindsdbcommunity/shared_invite/zt-o8mrmx3l-5ai~5H66s6wlxFfBMVI6wQ).
    
The `DROP` statement is used to delete an existing model table or delete datasource integration.

## DROP TABLE statement

The `DROP TABLE` statement is used to delete the model table:

```sql
DROP TABLE table_name;
```

### DROP TABLE example

The following SQL statement drops the model table called `home_rentals_model`:

```sql
DROP TABLE home_rentals_model;
```

## DROP DATABASE statement

The `DROP DATABASE` statement is used to delete datasource integration database:

```sql
DROP DATABASE integration_name;
```

### DROP DATABASE example

The following SQL statement drops the integration database called `psql_rentals`:

```sql
DROP TABLE psql_rentals;
```

# MindsDB Default Structure

On start-up, MindsDB consists of one system database (`information_schema`), one default project (`mindsdb`), and two base tables (`models` and `models_versions`) that belong to the default project.

You can verify it by running the following SQL commands:

```sql
SHOW [FULL] DATABASES;
```

On execution, we get:

```sql
+----------------------+---------+--------+
| Database             | TYPE    | ENGINE |
+----------------------+---------+--------+
| information_schema   | system  | [NULL] |
| mindsdb              | project | [NULL] |
| files                | data    | files  |
+----------------------+---------+--------+
```

And:

```sql
SHOW [FULL] TABLES;
```

On execution, we get:

```sql
+----------------------+-------------+
| Tables_in_mindsdb    | Table_type  |
+----------------------+-------------+
| models               | BASE TABLE  |
| models_versions      | BASE TABLE  |
+----------------------+-------------+
```

## The `information_schema` Database

The `information_schema` database contains all the system tables, such as `databases`, `tables`, `columns`, `ml_engines`, etc.

You can query for any system information using this query template:

```sql
SELECT *
FROM information_schema.table_name;
```

Don't forget to replace *table_name* with the table of your interest.

## The `mindsdb` Project

You create models and views within projects. The default project is `mindsdb`. But you can create your projects using the `CREATE DATABASE` statement, as below:

```sql
CREATE DATABASE my_new_project;
```

Here is how to create a model within your project:

```sql
CREATE MODEL my_new_project.my_model
FROM integration_name
    (SELECT * FROM table_name)
PREDICT target;
```

And here is how to create a view within your project:

```sql
CREATE VIEW my_new_project.my_view (
    SELECT *
    FROM integration_name.table_name
);
```

Please replace the *integration_name* and *table_name* placeholders with your database name and your table name respectively.

Now you can verify that the model and view are within your project:

```sql
SHOW FULL TABLES FROM my_new_project;
```

On execution, we get:

```sql
+------------------------------+-------------+
| Tables_in_my_new_project     | Table_type  |
+------------------------------+-------------+
| models                       | BASE TABLE  |
| models_versions              | BASE TABLE  |
| my_model                     | MODEL       |
| my_view                      | VIEW        |
+------------------------------+-------------+
```

## The `models` and `models_versions` Tables

The `mindsdb` project and every project that you create contain these two tables by default.

The `models` table stores information about the modes within the project, such as `name`, `status`, `accuracy`, and more.

The `models_versions` table stores information about all present and past versions of each model.

For more information on the `models` and `models_versions` tables, visit our docs on the `PROJECT` entity.

## The `files` Database

It is another default database that stores all the files uploaded by you to MindsDB Cloud.

Here is how you can [upload files to MindsDB](/sql/create/file/).

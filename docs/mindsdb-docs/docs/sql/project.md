# The `PROJECT` Entity

MindsDB introduces the `PROJECT` entity that lets you create projects to store your ML experiments.

## Working with `PROJECTS`

### Creating `PROJECTS`

You can create projects to store your models, making the structure like:

```
project_alpha
├─ models
├─ models_versions
├─ model_a
├─ model_b
project_beta
├─ models
├─ models_versions
├─ model_c
```

Here is how you create a project:

```sql
CREATE DATABASE project_alpha;
```

### Viewing `PROJECTS`

There are two ways you can list all your databases and projects:

1. Use the `SHOW DATABASES` command:

    ```sql
    SHOW DATABASES;
    ```

    On execution, we get:

    ```sql
    +----------------------+
    | Database             |
    +----------------------+
    | information_schema   |
    | mindsdb              |
    | project_alpha        |
    | project_beta         |
    +----------------------+
    ```

2. Use the `SHOW FULL DATABASES` command to get more details:

    ```sql
    SHOW FULL DATABASES;
    ```

    On execution, we get:

    ```sql
    +----------------------+----------+-----------+
    | Database             | TYPE     | ENGINE    |
    +----------------------+----------+-----------+
    | information_schema   | system   | [NULL]    |
    | mindsdb              | project  | [NULL]    |
    | project_alpha        | project  | [NULL]    |
    | project_beta         | project  | [NULL]    |
    +----------------------+----------+-----------+
    ```

### Dropping `PROJECTS`

Here is how you can remove a project:

```sql
DROP DATABASE project_alpha;
```

On execution, we get:

```sql
Query OK, 0 rows affected (x.xxx sec)
```

!!! note "Cannot Drop a Project"
    Please note that if your project stores at least one model, it cannot be removed. In this case, you should first drop all the models belonging to this project, and then, you can remove the project.

## Working with `MODELS`

### Creating `MODELS`

Here is how you create a model within the project:

```sql
CREATE MODEL project_alpha.model_a
FROM integration_name
    (SELECT * FROM table_name)
PREDICT target;
```

On execution, we get:

```sql
Query OK, 0 rows affected (x.xxx sec)
```

### Viewing `MODELS`

To see all the models from all projects, run the command below.

```sql
SHOW MODELS;
```

On execution, we get:

```sql
+---------+--------------+--------+--------+---------+-------------+---------------+------+------------------------+--------------------+
|NAME     |PROJECT       |STATUS  |ACCURACY|PREDICT  |UPDATE_STATUS|MINDSDB_VERSION|ERROR |SELECT_DATA_QUERY       |TRAINING_OPTIONS    |
+---------+--------------+--------+--------+---------+-------------+---------------+------+------------------------+--------------------+
|model_a  |project_alpha |complete|0.999   |target   |up_to_date   |22.10.2.1      |[NULL]|SELECT * FROM table_name|{'target': 'target'}|
|model_b  |project_alpha |complete|0.999   |target   |up_to_date   |22.10.2.1      |[NULL]|SELECT * FROM table_name|{'target': 'target'}|
|model_c  |project_beta  |complete|0.999   |target   |up_to_date   |22.10.2.1      |[NULL]|SELECT * FROM table_name|{'target': 'target'}|
+---------+--------------+--------+--------+---------+-------------+---------------+------+------------------------+--------------------+
```

And if you want to list all the models from a defined project, run either of the commands below.

```sql
SHOW MODELS 
FROM project_alpha;
-- or
SELECT * 
FROM project_alpha.models;
```

On execution, we get:

```sql
+---------+--------------+--------+--------+---------+-------------+---------------+------+------------------------+--------------------+
|NAME     |PROJECT       |STATUS  |ACCURACY|PREDICT  |UPDATE_STATUS|MINDSDB_VERSION|ERROR |SELECT_DATA_QUERY       |TRAINING_OPTIONS    |
+---------+--------------+--------+--------+---------+-------------+---------------+------+------------------------+--------------------+
|model_a  |project_alpha |complete|0.999   |target   |up_to_date   |22.10.2.1      |[NULL]|SELECT * FROM table_name|{'target': 'target'}|
|model_b  |project_alpha |complete|0.999   |target   |up_to_date   |22.10.2.1      |[NULL]|SELECT * FROM table_name|{'target': 'target'}|
+---------+--------------+--------+--------+---------+-------------+---------------+------+------------------------+--------------------+
```

Here is how to run a detailed search:

```sql
SHOW MODELS 
FROM project_alpha 
LIKE 'model_a' 
WHERE status='complete';
```

On execution, we get:

```sql
+---------+--------------+--------+--------+---------+-------------+---------------+------+------------------------+--------------------+
|NAME     |PROJECT       |STATUS  |ACCURACY|PREDICT  |UPDATE_STATUS|MINDSDB_VERSION|ERROR |SELECT_DATA_QUERY       |TRAINING_OPTIONS    |
+---------+--------------+--------+--------+---------+-------------+---------------+------+------------------------+--------------------+
|model_a  |project_alpha |complete|0.999   |target   |up_to_date   |22.10.2.1      |[NULL]|SELECT * FROM table_name|{'target': 'target'}|
+---------+--------------+--------+--------+---------+-------------+---------------+------+------------------------+--------------------+
```

### Dropping `MODELS`

To drop a model, run this command:

```sql
DROP MODEL project_alpha.model_a;
```

On execution, we get:

```sql
Query OK, 0 rows affected (x.xxx sec)
```

## Working with `MODELS_VERSION`

There is a `models_versions` table for each project that stores all the versions of your models.

Here is how to query for all model versions from all the projects:

```sql
SELECT * 
FROM information_schema.models_versions;
```

On execution, we get:

```sql
+-------+-------------+------+-------+--------+--------+-------+-------------+---------------+------+------------------------+--------------------+
|NAME   |PROJECT      |ACTIVE|VERSION|STATUS  |ACCURACY|PREDICT|UPDATE_STATUS|MINDSDB_VERSION|ERROR |SELECT_DATA_QUERY       |TRAINING_OPTIONS    |
+-------+-------------+------+-------+--------+--------+-------+-------------+---------------+------+------------------------+--------------------+
|model_a|project_alpha|true  |1      |complete|0.999   |target |up_to_date   |22.10.2.1      |[NULL]|SELECT * FROM table_name|{'target': 'target'}|
|model_b|project_alpha|true  |1      |complete|0.999   |target |up_to_date   |22.10.2.1      |[NULL]|SELECT * FROM table_name|{'target': 'target'}|
|model_c|project_beta |true  |1      |complete|0.999   |target |up_to_date   |22.10.2.1      |[NULL]|SELECT * FROM table_name|{'target': 'target'}|
+-------+-------------+------+-------+--------+--------+-------+-------------+---------------+------+------------------------+--------------------+
```

!!! note "Example"
    If there is more training data available, you don't need to recreate your model. Instead, use the `RETRAIN` command.

    ```sql
    RETRAIN project_alpha.model_b;
    ```

    After the retraining process completes, here is what you get:

    ```sql
    SELECT * 
    FROM information_schema.models_versions;
    ```

    On execution, we get:

    ```sql
    +-------+-------------+------+-------+--------+--------+-------+-------------+---------------+------+------------------------+--------------------+
    |NAME   |PROJECT      |ACTIVE|VERSION|STATUS  |ACCURACY|PREDICT|UPDATE_STATUS|MINDSDB_VERSION|ERROR |SELECT_DATA_QUERY       |TRAINING_OPTIONS    |
    +-------+-------------+------+-------+--------+--------+-------+-------------+---------------+------+------------------------+--------------------+
    |model_a|project_alpha|true  |1      |complete|0.999   |target |up_to_date   |22.10.2.1      |[NULL]|SELECT * FROM table_name|{'target': 'target'}|
    |model_b|project_alpha|false |1      |complete|0.999   |target |up_to_date   |22.10.2.1      |[NULL]|SELECT * FROM table_name|{'target': 'target'}|
    |model_b|project_alpha|true  |2      |complete|0.999   |target |up_to_date   |22.10.2.1      |[NULL]|SELECT * FROM table_name|{'target': 'target'}|
    |model_c|project_beta |true  |1      |complete|0.999   |target |up_to_date   |22.10.2.1      |[NULL]|SELECT * FROM table_name|{'target': 'target'}|
    +-------+-------------+------+-------+--------+--------+-------+-------------+---------------+------+------------------------+--------------------+
    ```

    Now, the `model_b` model has two records storing its two versions, out of which one is active.

You can also query for model versions of a project using this `SELECT` statement:

```sql
SELECT * FROM project_alpha.models_versions;
```

On execution, we get:

```sql
+-------+-------------+------+-------+--------+--------+-------+-------------+---------------+------+------------------------+--------------------+
|NAME   |PROJECT      |ACTIVE|VERSION|STATUS  |ACCURACY|PREDICT|UPDATE_STATUS|MINDSDB_VERSION|ERROR |SELECT_DATA_QUERY       |TRAINING_OPTIONS    |
+-------+-------------+------+-------+--------+--------+-------+-------------+---------------+------+------------------------+--------------------+
|model_a|project_alpha|true  |1      |complete|0.999   |target |up_to_date   |22.10.2.1      |[NULL]|SELECT * FROM table_name|{'target': 'target'}|
|model_b|project_alpha|false |1      |complete|0.999   |target |up_to_date   |22.10.2.1      |[NULL]|SELECT * FROM table_name|{'target': 'target'}|
|model_b|project_alpha|true  |2      |complete|0.999   |target |up_to_date   |22.10.2.1      |[NULL]|SELECT * FROM table_name|{'target': 'target'}|
+-------+-------------+------+-------+--------+--------+-------+-------------+---------------+------+------------------------+--------------------+
```

## Working with `TABLES`

The models that you create with the `CREATE MODEL` command are simple tables within a project. Therefore, you can use the `SHOW [FULL] TABLES` commands to query for them.

Here is how to query for tables from all databases/projects/schemas:

```sql
SELECT table_schema, table_name, table_type
FROM information_schema.tables 
WHERE table_type IN ('BASE TABLE', 'MODEL');
```

On execution, we get:

```sql
+--------------+----------------+------------+
|table_schema  |table_name      |table_type
+--------------+----------------+------------+
|mindsdb       |models          |BASE TABLE  |
|mindsdb       |models_versions |BASE TABLE  |
|project_alpha |models          |BASE TABLE  |
|project_alpha |models_versions |BASE TABLE  |
|project_beta  |models          |BASE TABLE  |
|project_beta  |models_versions |BASE TABLE  |
|project_alpha |model_a         |MODEL       |
|project_alpha |model_b         |MODEL       |
|project_beta  |model_c         |MODEL       |
+--------------+----------------+------------+
```

!!! note "Default Tables"
    Please note that each project contains two tables by default. These are the `models` table and the `models_versions` table.

There are also shortcut commands to query for the tables:

1. Querying for tables from the default project:

    ```sql
    SHOW TABLES;
    ```

    On execution, we get:

    ```sql
    +---------------------+
    |Tables_in_mindsdb    |
    +---------------------+
    |models               |
    |models_versions      |
    +---------------------+
    ```

    Or, to get more details:

    ```sql
    SHOW FULL TABLES;
    ```

    On execution, we get:

    ```sql
    +---------------------+-----------+
    |Tables_in_mindsdb    |Table_type |
    +---------------------+-----------+
    |models               |BASE TABLE |
    |models_versions      |BASE TABLE |
    +---------------------+-----------+
    ```

    !!! note "How to Set a Default Project"
        The default project is set to `mindsdb`. If you want to change it, run the `USE project_name;` command.

2. Querying for tables from a defined project:

    ```sql
    SHOW TABLES FROM project_alpha;
    ```

    On execution, we get:

    ```sql
    +-------------------------+
    |Tables_in_project_alpha  |
    +-------------------------+
    |models                   |
    |models_versions          |
    |model_a                  |
    |model_b                  |
    +-------------------------+
    ```

    Or, to get more details:

    ```sql
    SHOW FULL TABLES FROM project_alpha;
    ```

    On execution, we get:

    ```sql
    +-------------------------+-----------+
    |Tables_in_project_alpha  |Table_type |
    +-------------------------+-----------+
    |models                   |BASE TABLE |
    |models_versions          |BASE TABLE |
    |model_a                  |MODEL      |
    |model_b                  |MODEL      |
    +-------------------------+-----------+
    ```

# `CREATE ML_ENGINE` Statement

You can create machine learning (ML) engines based on the ML handlers available in MindsDB. And if you can't find the ML handler of your interest, you can always contribute by [building a new ML handler](/contribute/ml_handlers/).

## Description

The `CREATE ML_ENGINE` command creates an ML engine that uses one of the available ML handlers.

## Syntax

Before creating an ML engine, make sure that the ML handler of your interest is available by querying for the ML handlers.

```sql
SELECT *
FROM information_schema.handlers;
-- or 
SHOW HANDLERS;
```

!!! note "Can't Find an ML Handler?"
    If you can't find the ML handler of your interest, you can contribute by [building a new ML handler](/contribute/ml_handlers/).

If you find the ML handler of your interest, then you can create an ML engine using this command:

```sql
CREATE ML_ENGINE ml_engine_name
FROM handler_name
[USING argument_key = argument_value];
```

Please replace `ml_engine_name`, `handler_name`, and optionally, `argument_key` and `argument_value` with the real values.

To verify that your ML engine was successfully created, run the command below:

```sql
SELECT *
FROM information_schema.ml_engines;
-- or 
SHOW ML_ENGINES;
```

If you want to drop an ML engine, run the command below:

```sql
DROP ML_ENGINE ml_engine_name;
```

## Example

Let's check what ML handlers are currently available:

```sql
SHOW HANDLERS;
```

On execution, we get:

```sql
+------------------+------------+---------------------------------+-------+------------------------------------------------------------------------------+---------------+----------------------+------+
|NAME              |TITLE       |DESCRIPTION                      |VERSION|CONNECTION_ARGS                                                               |IMPORT_SUCCESS |IMPORT_ERROR          |FIELD8|
+------------------+------------+---------------------------------+-------+------------------------------------------------------------------------------+---------------+----------------------+------+
|merlion           |Merlion     |MindsDB handler for Merlion      |0.0.1  |[NULL]                                                                        |true           |[NULL]                |      |
|byom              |BYOM        |MindsDB handler for BYOM         |0.0.1  |{'model_code': {'type': 'path', 'description': 'The path name to model code'}}|true           |[NULL]                |      |
|ludwig            |Ludwig      |MindsDB handler for Ludwig AutoML|0.0.2  |[NULL]                                                                        |false          |No module named 'dask'|      |
|lightwood         |Lightwood   |[NULL]                           |1.0.0  |[NULL]                                                                        |true           |[NULL]                |      |
|huggingface       |Hugging Face|MindsDB handler for Higging Face |0.0.1  |[NULL]                                                                        |true           |[NULL]                |      |
+------------------+------------+---------------------------------+-------+------------------------------------------------------------------------------+---------------+----------------------+------+
```

Here we create an ML engine using the Lightwood handler.

```sql
CREATE ML_ENGINE my_lightwood_engine
FROM lightwood;
```

On execution, we get:

```sql
Query successfully completed
```

Now let's verify that our ML engine exists.

```sql
SHOW ML_ENGINES;
```

On execution, we get:

```sql
+-------------------+-----------+----------------+
|NAME               |HANDLER    |CONNECTION_DATA |
+-------------------+-----------+----------------+
|huggingface        |huggingface|{'password': ''}|
|lightwood          |lightwood  |{'password': ''}|
|my_lightwood_engine|lightwood  |{'password': ''}|
+-------------------+-----------+----------------+
```

Please note that we haven't used any arguments while creating the ML engine. The `USING` clause is optional, as it depends on the ML handler whether it requires/allows some arguments or not.

After creating your ML engine, you can create a model like this:

```sql
CREATE MODEL my_model
FROM integration_name
    (SELECT * FROM table_name)
USING engine = 'my_lightwood_engine';
```

The `USING` clause specifies the ML engine to be used for creating a new model.

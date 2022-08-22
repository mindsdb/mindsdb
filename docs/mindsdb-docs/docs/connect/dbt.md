# MindsDB and DBT

To integrate your predictions into your DBT workflow, use the dbt-mindsdb adapter:

| Adapter for                                                     | Documentation                                     | Install from PyPi                |
| --------------------------------------------------------------- | ------------------------------------------------- | -------------------------------- |
| MindsDB ([dbt-mindsdb](https://github.com/mindsdb/dbt-mindsdb)) | [Profile Setup](/connect/dbt/#initialization) | `#!bash pip install dbt-mindsdb` |

## Usage

### Initialization

1. Create dbt project:

   ```bash
   dbt init [project_name]
   ```

2. Configure your [profiles.yml](/connect/dbt-mindsdb-profile), 
   currently MindsDB only supports user/password authentication, as shown below on: `~/.dbt/profiles.yml`

=== "Self-Hosted Local Deployment"

    ```yml
    mindsdb:
      outputs:
        dev:
          type: mindsdb
          database: 'mindsdb'
          host: '127.0.0.1'
          port: 47335
          schema: 'mindsdb'
          password: ''
          username: 'mindsdb'
      target: dev
    ```

=== "MindsDB Cloud"

    ```yml
    mindsdb:
      outputs:
        dev:
          type: mindsdb
          database: 'mindsdb'
          host: 'cloud.mindsdb.com'
          port: 3306
          schema: '[dbt schema]'
          username: '[mindsdb cloud username]'
          password: '[mindsdb cloud password]'
      target: dev
    ```

| Key        | Required | Description                                          | Example                         |
| ---------- | :------: | ---------------------------------------------------- | ------------------------------- |
| `type`     |    ✔️    | The specific adapter to use                          | `mindsdb`                       |
| `host`     |    ✔️    | The MindsDB (hostname) to connect to                 | `cloud.mindsdb.com`             |
| `port`     |    ✔️    | The port to use                                      | `3306` or `47335`               |
| `schema`   |    ✔️    | Specify the schema (database) to build models into   | The MindsDB datasource          |
| `username` |    ✔️    | The username to use to connect to the server         | `mindsdb` or mindsdb cloud user |
| `password` |    ✔️    | The password to use for authenticating to the server | `pass`                          |

### Create a Predictor

Create table_name.sql (<em>table_name</em> will be used as the name of the predictor):

| Parameter       | Required | Description                                                                                        | Example          |
| --------------- | :------: | -------------------------------------------------------------------------------------------------- | ---------------- |
| `materialized`  |    ✔️    | Always `predictor`                                                                                 | `predictor`      |
| `integration`   |    ✔️    | Name of integration to get data from and save result to. It must be created in MindsDB beforehand. | `photorep`       |
| `predict`       |    ✔️    | Field to be predicted                                                                              | `name`           |
| `predict_alias` |          | Alias for predicted field                                                                          | `predicted_name` |
| `using`         |          | Configuration options for trained model                                                            | ...              |

```sql
{{
    config(
        materialized='predictor',
        integration='photorep',
        predict='name',
        predict_alias='predicted_name',
        using={
            'encoders.location.module': 'CategoricalAutoEncoder',
            'encoders.rental_price.module': 'NumericEncoder'
        }
    )
}}
    SELECT *
    FROM stores;
```

### Create Predictions Table

Create <em>table_name</em>.sql (If you need to specify schema, you can do it with a dot separator: <em><strong>schema_name.</strong>table_name</em>.sql):

| Parameter        | Required | Description                                                                                        | Example           |
| ---------------- | :------: | -------------------------------------------------------------------------------------------------- | ----------------- |
| `materialized`   |    ✔️    | Always `table`                                                                                     | `table`           |
| `predictor_name` |    ✔️    | Name of predictor model from `Create predictor`                                                    | `store_predictor` |
| `integration`    |    ✔️    | Name of integration to get data from and save result to. It must be created in MindsDB beforehand. | `photorep`        |

```sql
{{
    config(
        materialized='table',
        predictor_name='store_predictor',
        integration='photorep'
    )
}}
    SELECT a, bc
    FROM ddd
    WHERE name > latest;
```

!!! warning "Note that each time dbt is run, the results table will be rewritten."

## Testing

1. Install dev requirements

   ```bash
   pip install -r dev_requirements.txt
   ```

2. Run pytest

   ```bash
   python -m pytest tests/
   ```

## What's Next?

Now that you are all set, we recommend you check out our **Tutorials** and **Community Tutorials** sections, where you'll find various examples of regression, classification, and time series predictions with MindsDB.

To learn more about MindsDB itself, follow the guide on [MindsDB database structure](/sql/table-structure/). Also, don't miss out on the remaining pages from the **SQL API** section, as they explain a common SQL syntax with examples.

Have fun!

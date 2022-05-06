To integrate your predictions into your DBT workflow, use the dbt-mindsdb adapter:

| Adapter for      | Documentation                          | Install from PyPi |
| ---------------- | ------------------------------------ | ----- |
| MindsDB ([dbt-mindsdb](https://github.com/mindsdb/dbt-mindsdb))       | [Profile Setup](/sql/connect/dbt-mindsdb-profile)  | `pip install dbt-mindsdb` |

## Usage

### Initialization

1. Create dbt project:
```
    dbt init <project_name>
```
1. Configure your [profiles.yml](/sql/connect/dbt-mindsdb-profile)

### Create predictor

1. Create <em>table_name</em>.sql (<em>table_name</em> will be used as the name of the predictor):

| Parameter     | Required | Description                                          | Example                        |
| ------------- | -------- | ---------------------------------------------------- | ------------------------------ |
| materialized  |     ✔️    | Always `predictor`                                   | `predictor`                    |
| integration   |     ✔️    | Name of integration to get data from and save result to.  It must be created in MindsDB beforehand.                 | `photorep`            |
| predict       |     ✔️    | Field to be predicted                                      | `name`             |
| predict_alias |          | Alias for predicted field   | `predicted_name`         |
| using         |          | Configuration options for trained model         | ... |

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
      select * from stores
```

### Create predictions table

1. Create <em>table_name</em>.sql (If you need to specify schema, you can do it with a dot separator: <em><strong>schema_name.</strong>table_name</em>.sql):

| Parameter       | Required | Description                                          | Example                        |
| --------------- | -------- | ---------------------------------------------------- | ------------------------------ |
| materialized    |     ✔️    | Always `table`                                       | `table`                        |
| predictor_name  |     ✔️    | Name of predictor model from `Create predictor`      | `store_predictor`                   |
| integration     |     ✔️    | Name of integration to get data from and save result to.  It must be created in MindsDB beforehand.                 | `photorep`            |

```sql
    {{ config(materialized='table', predictor_name='store_predictor', integration='photorep') }}
        select a, bc from ddd where name > latest
```

Note that each time dbt is run, the results table will be rewritten.

## Testing

- Install dev requirements
```
  pip install -r dev_requirements.txt
```
- Run pytest
```
  python -m pytest tests/
```
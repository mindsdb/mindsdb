# MindsDB and OpenAI

You can bring OpenAI models as tables to mindsDB, here is an example:

```sql
CREATE PREDICTOR mindsdb.byom_mlflow 
PREDICT target_text_column  -- `1` is the target column name
USING 
    format='openai',
    APITOKEN='yourapitoken'
    data_dtype={"0": "integer", "1": "integer"}
```

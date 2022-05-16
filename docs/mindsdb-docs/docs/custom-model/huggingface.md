# MindsDB and HuggingFace

You can bring your Hugging face models as tables to mindsDB, here is an example:

```sql
CREATE PREDICTOR mindsdb.byom_mlflow 
PREDICT `1`  -- `1` is the target column name
USING 
format='huggingfae',
APITOEKN='yourapitoken'
data_dtype={"0": "integer", "1": "integer"}
```


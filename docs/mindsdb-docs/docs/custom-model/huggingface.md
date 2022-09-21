# MindsDB and HuggingFace

HuggingFace facilitates building, training, and deploying ML models.

## How to Bring the HuggingFace Model to MindsDB

To bring your HuggingFace model to MindsDB, run the `CREATE PREDICTOR` statement as below.

```sql
CREATE PREDICTOR mindsdb.huggingface_model
PREDICT `1`  -- `1` is the target column name
USING 
    format='huggingface',
    APITOKEN='yourapitoken'
    data_dtype={"0": "integer", "1": "integer"};
```

Now you can query the `mindsdb.predictors` table to see your model.

```sql
SELECT *
FROM mindsdb.predictors
WHERE name='huggingface_model';
```

Check out the guide on the [`SELECT`](/sql/api/select/) statement to see how to get the predictions.

# MindsDB and OpenAI

OpenAI facilitates building and deploying ML models.

## How to Bring the OpenAI Model to MindsDB

To bring your OpenAI model to MindsDB, run the `CREATE PREDICTOR` statement as below.

```sql
CREATE PREDICTOR mindsdb.openai_model
PREDICT target_text_column
USING 
    format='openai',
    APITOKEN='yourapitoken'
    data_dtype={"0": "integer", "1": "integer"};
```

Now you can query the `mindsdb.predictors` table to see your model.

```sql
SELECT *
FROM mindsdb.predictors
WHERE name='openai_model';
```

Check out the guide on the [`SELECT`](/sql/api/select/) statement to see how to get the predictions.

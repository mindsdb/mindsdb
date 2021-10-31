# RETRAIN PREDICTOR Statement

The `RETRAIN` statement is used to retrain old predictors. The basic syntax for retraining the predictors is:

```sql
RETRAIN predictor_name;
```
The predictor is updated to leverage any new data in optimizing its predictive capabilities, without necessarily taking as long to train as starting from scratch.

### RETRAIN Predictor example

This example shows how you can retrain the predictor called `home_rentals_model`.

```sql
RETRAIN home_rentals_model;
```

### SELECT Predictor status

To check if the status of the predictor is outdated you can `SELECT` from predictors table:

```sql
SELECT * FROM mindsdb.predictors WHERE name='predictor_name';
```

### SELECT Predictor example

To check the status of the `home_rentals_model` run:

```sql
SELECT * FROM mindsdb.predictors WHERE name='home_rentals_model';
```

![Model Sttus](/assets/sql/status.png)

If the status is `OUTDATED` you can retrain the predictor.
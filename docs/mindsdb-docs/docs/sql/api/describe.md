# DESCRIBE statement

The `DESCRIBE ...` statement is used to display the attributes of an existing model.

## DESCRIBE FEATURES


The `DESCRIBE [...].features` statement is used to display the way that the model encoded the data prior to training.

### Syntax:

```sql
DESCRIBE [name_of_your_predictor].features;
```
### Expected Output

* column: name of the feature 
* type: binary integer... 
* encoder: name of the encoder used for that column
* role: describes whether the column is a target or a feature

## DESCRIBE MODEL  

`DESCRIBE [...].model` statement is used to display the performance of the candidate models.

### Syntax:

```sql
DESCRIBE [name_of_your _predictor].model;
```

### Expected Output


* name: name of the model
* performance : obtained accuracy (from 0.0 to 1.0) for that candidate model
* selected: the Auto ML pick the best performed cadidate (1)
* training_time: training time for that condidate model

```sql
DESCRIBE [name_of_your _predictor].ensemble;
```

### Expected Output

ensemble: ensemble type used to select best model candidate

If you're unsure on how to DESCRIBE your model or understand the results feel free to ask us how to do it on the community [Slack workspace](https://join.slack.com/t/mindsdbcommunity/shared_invite/zt-o8mrmx3l-5ai~5H66s6wlxFfBMVI6wQ).

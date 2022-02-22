# Pre-requisites

Before you start make sure that you've:

1. Visited [Getting Started Guide](/info)
2. Visited [Getting Started with Cloud](/deployment/cloud)
3. Downloaded the dataset. You can get it from [Kaggle](https://www.kaggle.com/edumagalhaes/quality-prediction-in-a-mining-process).

# Manufacturing process quality

Predicting process result quality is a common task in manufacturing analytics. Manufacturing plants commonly use quality predictions to gain a competitive edge over their competitors, improve their products or increase their customers satisfaction. **MindsDB** is a tool that can help you solve quality prediction tasks **easily** and **effectively** using machine learning. 
MindsDB abstracts ML models as virtual “AI Tables” in databases and you can make predictions just using normal SQL commands.

In this tutorial you will learn how to predict the quality of a mining process using **MindsDB**.

## Upload a file

1. Fix headers: 
   - `sed -e 's/ /_/g' -e 's/\(.*\)/\L\1/' -e 's/%_//g' MiningProcess_Flotation_Plant_Database.csv > fixed_headers.csv` (for Linux/Unix)
   - edit headers manually: change `space` to `underscore`, upper case to lower case, remove `%` from headers (for Windows)
2. Click on **Files** icon to go to datasets page
3. Click on **FILE UPLOAD** button to upload file into MindsDB


## Connect to MindsDB SQL Sever
1. 
```sql
mysql -h cloud.mindsdb.com --port 3306 -u username@email.com -p
```
2. 
```sql
USE mindsdb;
```

## Create a predictor

In this section you will connect to MindsDB with the MySql API and create a Predictor. It is in MindsDB terms a machine learning model, but all its complexity is automated and abstracted as a virtual “AI Table”. If you are an ML expert and want to tweak the model, MindsDB also allows you that (please refer to documentation).

Use the following query to create a Predictor that will foretell the silica_concentrate at the end of our mining process.
> The row number is limited to 5000 to speed up training but you can keep the whole dataset.
```sql
CREATE PREDICTOR process_quality_predictor
FROM files (
    SELECT iron_feed, silica_feed, starch_flow, amina_flow, ore_pulp_flow,
           ore_pulp_ph, ore_pulp_density,flotation_column_01_air_flow,
           flotation_column_02_air_flow, flotation_column_03_air_flow,
           flotation_column_04_air_flow, flotation_column_05_air_flow,
           flotation_column_06_air_flow,flotation_column_07_air_flow,
           flotation_column_01_level, flotation_column_02_level,
           flotation_column_03_level, flotation_column_04_level,
           flotation_column_05_level, flotation_column_06_level, 
           flotation_column_07_level, iron_concentrate, silica_concentrate from process_quality 
    FROM process_quality LIMIT 5000
) PREDICT silica_concentrate as quality USING;
```

After creating the Predictor you should see a similar output:

```console
Query OK, 0 rows affected (2 min 27.52 sec)
```

Now the Predictor will begin training. You can check the status with the following query.

```sql
SELECT * FROM mindsdb.predictors WHERE name='process_quality_predictor';
```

After the Predictor has finished training, you will see a similar output.

```console
+-----------------------------+----------+----------+--------------------+-------------------+------------------+
| name                        | status   | accuracy | predict            | select_data_query | training_options |
+-----------------------------+----------+----------+--------------------+-------------------+------------------+
| process_quality_predictor   | complete | 1        | silica_concentrate |                   |                  |
+-----------------------------+----------+----------+--------------------+-------------------+------------------+
1 row in set (0.28 sec)
```

As you can see the accuracy of the model is 1 (i.e. 100%). This is the result of using a limited dataset of 5000 rows. In reality when using the whole dataset, you will probably see a more reasonable accuracy.

You are now done with creating the predictor! ✨

## Make predictions

In this section you will learn how to make predictions using your trained model.

To run a prediction against new or existing data, you can use the following query.

```sql
SELECT silica_concentrate, silica_concentrate_confidence, silica_concentrate_explain as Info
FROM mindsdb.process_quality_predictor
WHERE when_data='{"iron_feed": 48.81, "silica_feed": 25.31, "starch_flow": 2504.94, "amina_flow": 309.448, "ore_pulp_flow": 377.6511682692, "ore_pulp_ph": 10.0607, "ore_pulp_density": 1.68676}';
```

The output should look similar to this.
```console
+--------------------+-------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------+
| silica_concentrate | silica_concentrate_confidence | Info                                                                                                                                            |
+--------------------+-------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------+
| 1.68               | 0.99                          | {"predicted_value": "1.68", "confidence": 0.99, "confidence_lower_bound": null, "confidence_upper_bound": null, "anomaly": null, "truth": null} |
+--------------------+-------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------+
1 row in set (0.81 sec)
```

As you can see, the model predicted the `silica concentrate` for our data point. Again we can see a very high confidence due to the limited dataset. When making predictions you can include different fields. As you can notice, we have only included the first 7 fields of our dataset. You are free to test different combinations.

In the previous example, we have made a prediction for a single data point. In a real scenario, you might want to make predictions on multiple data points. In this case, MindsDB allows you to Join this other table with the Predictor. In result, you will get another table as an output with a predicted value as one of its columns.

Let’s see how to make batch predictions.

Use the following command to create the batch prediction.

```sql
SELECT 
    collected_data.iron_feed,
    collected_data.silica_feed,
    collected_data.starch_flow,
    collected_data.amina_flow,
    collected_data.ore_pulp_flow,
    collected_data.ore_pulp_ph,
    collected_data.ore_pulp_density,
    predictions.silica_concentrate_confidence as confidence,
    predictions.silica_concentrate as predicted_silica_concentrate
FROM process_quality_integration.process_quality AS collected_data
JOIN mindsdb.process_quality_predictor AS predictions
LIMIT 5;
```

As you can see below, the predictor has made multiple predictions for each data point in the `collected_data` table! You can also try selecting other fields to get more insight on the predictions. See the [JOIN clause documentation](https://docs.mindsdb.com/sql/api/join/) for more information.

```console
+-----------+-------------+-------------+------------+---------------+-------------+------------------+------------+------------------------------+
| iron_feed | silica_feed | starch_flow | amina_flow | ore_pulp_flow | ore_pulp_ph | ore_pulp_density | confidence | predicted_silica_concentrate |
+-----------+-------------+-------------+------------+---------------+-------------+------------------+------------+------------------------------+
| 58.84     | 11.46       | 3277.34     | 564.209    | 403.242       | 9.88472     | 1.76297          | 0.99       | 2.129567174379606            |
| 58.84     | 11.46       | 3333.59     | 565.308    | 401.016       | 9.88543     | 1.76331          | 0.99       | 2.129548423407259            |
| 58.84     | 11.46       | 3400.39     | 565.674    | 399.551       | 9.88613     | 1.76366          | 0.99       | 2.130100408285386            |
| 58.84     | 11.46       | 3410.55     | 563.843    | 397.559       | 9.88684     | 1.764            | 0.99       | 2.1298757513510136           |
| 58.84     | 11.46       | 3408.98     | 559.57     | 401.719       | 9.88755     | 1.76434          | 0.99       | 2.130438907683961            |
+-----------+-------------+-------------+------------+---------------+-------------+------------------+------------+------------------------------+
```

You are now done with the tutorial! 🎉

Please feel free to try it yourself. Sign up for a [free MindsDB account](https://cloud.mindsdb.com/signup?utm_medium=community&utm_source=ext.%20blogs&utm_campaign=blog-manufacturing-process-quality) to get up and running in 5 minutes, and if you need any help, feel free to ask in [Slack](https://join.slack.com/t/mindsdbcommunity/shared_invite/zt-o8mrmx3l-5ai~5H66s6wlxFfBMVI6wQ) or [Github](https://github.com/mindsdb/mindsdb/discussions).

For more tutorials like this, check out [MindsDB documentation](https://docs.mindsdb.com/).

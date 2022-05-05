# Pre-requisites
Before you start make sure that you've:

- Visited [Getting Started Guide](/info)
- Visited [Getting Started with Cloud](/deployment/cloud)
- Downloaded the dataset. The dataset being used for this tutorial. Get it from [Kaggle](https://www.kaggle.com/fedesoriano/body-fat-prediction-dataset).

# Determining Body Fat Percentage

Machine Learning powered data analysis can be performed quickly and efficiently by MindsDB to enable individuals to make accurate predictions for certain metrics based on a variety of associated values. MindsDB enables you to make predictions automatically using just SQL commands, all the ML workflow is automated, and abstracted as virtual “AI tables” in your database so you may start getting insights from forecasts right away. In this tutorial, we'll be using MindsDB and a MySQL database to predict body fat percentage based on several body part measurement criteria.

### Data Overview
For this tutorial, we'll be using the Body Fat Prediction dataset available at [Kaggle](https://www.kaggle.com/fedesoriano/body-fat-prediction-dataset).  Each row represents one person and we'll train an ML model to help us predict an individual's body fat percentage using MindsDB.  Below is a short description of each feature of the data:

- Density: Individual's body density as determined by underwater weighing (float)
- BodyFat: The individual's determined body fat percentage (float).  This is what we want to predict
- Age: Age of the individual (int)
- Weight: Weight of the individual in pounds (float)
- Height: Height of the individual in inches (float)
- Neck: Circumference of the individual's neck in cm (float)
- Chest: Circumference of the individual's chest in cm (float)
- Abdomen: Circumference of the individual's abdomen in cm (float)
- Hip: Circumference of the individual's hips in cm (float)
- Thigh: Circumference of the individual's thigh in cm (float)
- Knee: Circumference of the individual's knee in cm (float)
- Ankle: Circumference of the individual's ankle in cm (float)
- Biceps: Circumference of the individual's biceps in cm (float)
- Forearm: Circumference of the individual's forearm in cm (float)
- Wrist: Circumference of the individual's wrist in cm (float)

## Upload a file

1. Click on **Files** icon to go to datasets page
2. Click on **FILE UPLOAD** button to upload file into MindsDB


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

Now, we have to create a predictor based on the following syntax:

```sql
CREATE PREDICTOR mindsdb.predictor_name
FROM files 
(SELECT column_name, column_name2 FROM file_name) as ds_name
PREDICT column_name as column_alias;
```

For our case, we'll enter the following command:
```sql
CREATE PREDICTOR mindsdb.bodyfat_predictor
FROM files (
        SELECT * FROM bodyfat
) PREDICT Bodyfat;
```

You should see output similar to the following:
```console
Query OK, 0 rows affected (3.077 sec)
```

At this point, the predictor will immediately begin training.  Check the status of the training by entering the command:

```sql
SELECT * FROM mindsdb.predictors WHERE name='bodyfat_predictor';
```

When complete, you should see output similar to the following:

```console
+-------------------+----------+--------------------+---------+-------------------+------------------+
| name              | status   | accuracy           | predict | select_data_query | training_options |
+-------------------+----------+--------------------+---------+-------------------+------------------+
| bodyfat_predictor | complete | 0.9909730079130395 | BodyFat |                   |                  |
+-------------------+----------+--------------------+---------+-------------------+------------------+
1 row in set (0.101 sec)
```

As you can see, the predictor training has completed with an accuracy of approximately 99%.  At this point, you have successfully trained an ML model for our Body Fat Prediction dataset!

## Using SQL Commands to Make Predictions
Now, we can query the model and make predictions based on our input data by using SQL statements.  

Let's imagine an individual aged 25, with a body density of 1.08, a weight of 170lb, a height of 70in, a neck circumference of 38.1cm, a chest circumference of 103.5cm, an abdomen circumference of 85.4cm, a hip circumference of 102.2cm, a thigh circumference of 63.0cm, a knee circumference of 39.4cm, an ankle circumference of 22.8cm, a biceps circumference of 33.3cm, a forearm circumference of 28.7cm, and a wrist circumference of 18.3cm.  We can predict this person's body fat percentage by issuing the following command:

```sql
SELECT BodyFat, BodyFat_confidence, BodyFat_explain 
FROM mindsdb.bodyfat_predictor 
WHERE Density=1.08 AND Age=25 AND Weight=170 AND Height=70 AND Neck=38.1 AND Chest=103.5 AND Abdomen=85.4 AND Hip=102.2 AND Thigh=63.0 AND Knee=39.4 AND Ankle=22.8 AND Biceps=33.3 AND Forearm=28.7 AND Wrist=18.3;
```

This should return output similar to:

```console
+-------------------+--------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| BodyFat           | BodyFat_confidence | Info                                                                                                                                                                                  |
+-------------------+--------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 8.968581383955318 | 0.99               | {"predicted_value": 8.968581383955318, "confidence": 0.99, "confidence_lower_bound": 5.758912817402102, "confidence_upper_bound": 12.178249950508533, "anomaly": null, "truth": null} |
+-------------------+--------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
1 row in set (0.464 sec)

```

As you can see, with around 99% confidence, MindsDB predicted the body fat percentage for this individual at 8.97%.  You can at this point feel free to alter the prospective individual's bodypart measurement parameters and make additional prediction queries if you'd like.  

### Making Batch Predictions using the JOIN Command
The above example showed how to make predictions for a single individual's bodyfat, but what if you had a table of bodypart measurements for a number of individuals, and wanted to make predictions for them all?  This is possible using the [JOIN command](https://docs.mindsdb.com/sql/api/join/), which allows for the combining of rows from a database table and the prediction model table on a related column.  

The basic syntax to use the JOIN command is:
```sql
SELECT t.column_name1, t.column_name2, FROM integration_name.table AS t 
JOIN mindsdb.predictor_name AS p WHERE t.column_name IN (value1, value2, ...);
```

For our purposes, we'll re-use the original data set, taking the Age, Density, Weight, Height, Neck circumference, Chest circumference, Abdomen circumference, and Hip circumference fields.  We'll also include the original BodyFat percentage to compare our predicted values against the originals.  Execute the following command:

```sql
SELECT t.Age, t.Density, t.Weight, t.Height, t.Neck, t.Chest, t.Abdomen, t.Hip, t.BodyFat, p.BodyFat AS predicted_BodyFat
FROM bodyfat_integration.bodyfat AS t JOIN mindsdb.bodyfat_predictor AS p
LIMIT 5;
```

This should return an output table similar to the following:
```console
+------+---------+--------+--------+------+-------+---------+-------+---------+--------------------+
| Age  | Density | Weight | Height | Neck | Chest | Abdomen | Hip   | BodyFat | predicted_BodyFat  |
+------+---------+--------+--------+------+-------+---------+-------+---------+--------------------+
| 23   | 1.0708  | 154.25 | 67.75  | 36.2 | 93.1  | 85.2    | 94.5  | 12.3    | 12.475132275112655 |
| 22   | 1.0853  | 173.25 | 72.25  | 38.5 | 93.6  | 83.0    | 98.7  | 6.1     | 6.07133439184195   |
| 22   | 1.0414  | 154.0  | 66.25  | 34.0 | 95.8  | 87.9    | 99.2  | 25.3    | 25.156538398443754 |
| 26   | 1.0751  | 184.75 | 72.25  | 37.4 | 101.8 | 86.4    | 101.2 | 10.4    | 10.696461885516461 |
| 24   | 1.034   | 184.25 | 71.25  | 34.4 | 97.3  | 100.0   | 101.9 | 28.7    | 28.498772660802427 |
+------+---------+--------+--------+------+-------+---------+-------+---------+--------------------+
5 rows in set (1.091 sec)
```

As you can see, a prediction has been generated for each row in the input table.  Additionally, our predicted bodyfat percentages align closely with the original values!  Note that even though we chose only to display the Age, Density, Weight, Height, Neck, Chest, Abdomen, and Hip measurements in this example, the predicted_BodyFat field was determined by taking into consideration all of the data fields in the original bodyfat table (as this table was JOINed with the bodyfat_predictor table, from which we selected the specified fields).  In order to make predictions based ONLY on the specified fields, we would have to create a new table containing only those fields, and JOIN that with the bodyfat_predictor table!

You are now done with the tutorial! 🎉

Please feel free to try it yourself. Sign up for a [free MindsDB account](https://cloud.mindsdb.com) to get up and running in 5 minutes, and if you need any help, feel free to ask in [Slack](https://join.slack.com/t/mindsdbcommunity/shared_invite/zt-o8mrmx3l-5ai~5H66s6wlxFfBMVI6wQ) or [Github](https://github.com/mindsdb/mindsdb/discussions).

For more tutorials like this check out [MindsDB documentation](https://docs.mindsdb.com/).

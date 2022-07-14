# Forecast Eye State From Electroencephalogram Readings

## Introduction

In this short example we will produce categorical forecasts for a multivariate time series.

## Data and Setup

The dataset we will use is the UCI "EEG Eye State" found [here](https://archive.ics.uci.edu/ml/datasets/EEG+Eye+State). Each row contains 15 different electroencephalogram (EEG) readings plus the current state of one of the patient's eye (0: closed, 1: open). We want to know ahead of time when the eye is going to change state, let's try!

Make sure you have access to a working MindsDB installation (either local or via [cloud.mindsdb.com](https://cloud.mindsdb.com)), and either load it into a table in your database of choice or upload the file directly to the special `FILES` datasource (via SQL or GUI). Once you've done this, proceed to the next step. For the rest of the tutorial we'll assume you've opted for the latter option and uploaded the file with the name `EEGEye`:

```sql
SHOW TABLES FROM files;

SELECT * FROM files.EEGEye LIMIT 10;
```

## Create a Time Series Predictor

Let's specify the `eyeDetection` column as our target variable. The interesting thing about this example is that we are aiming to forecast "labels" that are not strictly numerical. Even though this example is simple (because the variable is a binary category, either "Open"ed or "Close"d eye), this can easily be generalized to more categories.

We will order the measurements by the `Timestamps` column, which shows readings a frequency of approximately 8 miliseconds.

To create our predictor, we can execute the following statement:

```sql
CREATE PREDICTOR 
  mindsdb.eeg_eye_forecast
FROM files
  (SELECT * FROM EEGEye)
PREDICT eyeDetection
ORDER BY Timestamps
WINDOW 50
HORIZON 10;
```

As the sampling frequency is 8 ms, this predictor will use be trained using a historical context of roughly `(50 * 8) = 400 [ms]` to predict the following `(10 * 8) = 80 [ms]`.

You can check the status of the predictor:

```sql
SELECT * FROM mindsdb.predictors
WHERE name='eeg_eye_forecast';
```

## Generating Forecasts

Once the predictor has been successfully trained, you can query it to get predictions for the next `#!sql HORIZON` timesteps into the future, which in this case is roughly 80 miliseconds. Usually, in forecasting scenarios you'll want to know what happens right after the latest data measurement, for which we have a special bit of syntax, the `#!sql LATEST` key word:

```
SELECT m.Timestamps as timestamps,
       m.eyeDetection as eye_status
FROM files.EEGEye as t
JOIN mindsdb.eeg_eye_forecast as m  
WHERE t.timestamps > LATEST
LIMIT 10;
```

That's it. We can now `#!sql JOIN` any set of `#!sql WINDOW` rows worth of measurements with this predictor, and forecasts will be emitted to help us expect a change in the state of the patient's eye based on her EEG readings.

## Alternate Problem Framings

It is also possible to reframe this task as a "normal" forecasting scenario where the variable is numeric. There are a few options here, and it boils down to what the broader scenario is, and what format would maximize value of any specific prediction.

For example, a simple mapping of "eye is closed" to zero, and "eye is open" to one, would be enough to replicate the above behavior.

We could also explore other options. With some data transformations on the data layer, we could get a countdown to the next change in state, effectively predicting a "date" if we cast this back into the timestamp domain.

## Next Steps

We invite you to try this tutorial with your own time series data. If you have any questions or feedback, be sure to join our slack community or make an issue in the github repository!

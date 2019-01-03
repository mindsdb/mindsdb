[<Back to TOC](../README.md)
# MindsDB Time Series

Here you will find a file [fuel.csv](https://raw.githubusercontent.com/mindsdb/mindsdb/master/docs/examples/time_series/fuel.csv), containing the historical fuel consumption data.

### Goal
The goal is to be able to predict the fuel consumption of a vessel, given the past 24hrs of historical data.

#### Learning


```python

from mindsdb import *


MindsDB().learn(
    predict = 'Main_Engine_Fuel_Consumption_MT_day',
    from_data = 'fuel.csv',
    model_name='fuel',

    # Time series arguments:

    order_by='Time',
    group_by='id',
    window_size=24, # just 24 hours

)

```

So here the important lesson, is the extra arguments. 



    order_by='Time',
    group_by='id',
    window_size=24, # just 24 hours


These are tell mindsDB that it should consider the previous 24 rows (ordered by time, and grouped by the vessel id) in order to make a prediction.

#### Predicting

In order ot make a prediction, you can now pass a data frame with the last x hours of readings.

For simplicity we use a file.

Here you will find a file [fuel_predict.csv](https://raw.githubusercontent.com/mindsdb/mindsdb/master/docs/examples/time_series/fuel.csv), containing the last 24hrs and the asking for the subsequent hours.


```python

from mindsdb import *

# Here we use the model to make predictions (NOTE: You need to run train.py first)
result = MindsDB().predict(predict='Main_Engine_Fuel_Consumption_MT_day', model_name='fuel', from_data = 'fuel_predict.csv')

# you can now print the results
print('The predicted main engine fuel consumption')
print(result.predicted_values)


```

## Notes

#### About the Learning

Note: that the argument **from_data** can be a pandas data_frame, a path to a file or a URL

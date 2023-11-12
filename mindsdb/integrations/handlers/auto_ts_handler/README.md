### Briefly describe what ML framework does this handler integrate to MindsDB, and how?
Auto_timeseries is a complex model building utility for time series data. Since it automates many 
Tasks involved in a complex endeavor, it assumes many intelligent defaults. But you can change them. 
Auto_Timeseries will rapidly build predictive models based on Statsmodels ARIMA, Seasonal ARIMA, Prophet 
and Scikit-Learn ML. It will automatically select the best model which gives best score specified.



Call this handler with
`USING ENGINE='auto_ts'` 

### Why is this integration useful? What does the ideal predictive use case for this integration look like? When would you definitely not use this integration?
This integration is useful for time series data. It will automatically select the best model which gives best score specified. And it integrations with best Time Series 
libraries like Statsmodels ARIMA, Seasonal ARIMA, Prophet and Scikit-Learn.

If there is more than one target variable in your data set, just specify only one for now, and if you know the time interval that is in your data, you can specify it.
Otherwise it auto-ts will try to infer the time interval on its own

### Are models created with this integration fast and scalable, in general?
The model training time is fast and scalable as it uses best Time Series libraries like Statsmodels ARIMA, Seasonal ARIMA, Prophet and Scikit-Learn ML.
As Auto_ts uses doesn't use neural networks, it is not prone to overfitting and take less time to train.


### What are the recommended system specifications for models created with this framework?
No CUDA enabled GPU is required. It can be run on any machine. 
Some users can face issues while installing the package, [refer](https://github.com/AutoViML/Auto_TS#install)

### To what degree can users control the underlying framework by passing parameters via the USING syntax?
Almost all the inputs that Auto_TS takes can be passed via the USING syntax. 


``` sql
USING
    engine='auto_ts',
    time_interval = 'M',
    cv = 5,
    score_type = 'rmse',
    non_seasonal_pdq = 'None',
    seasonal_period = 12
```
target: The name of the column which needs to be predicted. It should be a numeric column.<br>
ts_column: The name of the column which denotes the time period. It should be a date column.<br>
cv: Number of cross-validation folds. Default is 5.<br>
score_type: The metric to be used for scoring models. Default is rmse.<br>
non_seasonal_pdq: The order of the non-seasonal ARIMA component. Default is None.<br>
seasonal_period: The period of the seasonal component.<br>


### Does this integration offer model explainability or insights via the DESCRIBE syntax?
Yes, it offers model explainability via the DESCRIBE syntax. 

``` sql
DESCRIBE
    predict_sales
```

### Does this integration support fine-tuning pre-existing models (i.e. is the update() method implemented)? Are there any caveats?
Not implemented yet.


# Please provide a minimal SQL example that uses this ML engine (pointers to integration tests in the PR also valid)
Add the data in mindsdb [data](https://drive.google.com/drive/folders/1qPHpsmIuSvC1FiMB5Y1kOh3rFp4Pv-3A?usp=sharing)

And follow the below SQL query

``` sql
CREATE MODEL mindsdb.sales_model
FROM files
  (SELECT * FROM Sales_and_Marketing)
PREDICT sales
ORDER BY time_period
USING
    engine='auto_ts',
    time_interval = 'M',
    cv = 5,
    score_type = 'rmse',
    non_seasonal_pdq = 'None',
    seasonal_period = 12
```
The above query will create a model in mindsdb

``` sql
SELECT sales_preds
FROM mindsdb.sales_model
WHERE time_period = '2013-04-01'
AND marketing_expense = 256;
```
The above query will predict for single row

```sql
SELECT m.sales_preds
FROM files.sales as t
JOIN mindsdb.sale_model as m ;
```


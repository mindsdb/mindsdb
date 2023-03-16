# Briefly describe what ML framework does this handler integrate to MindsDB, and how?
StatsForecast is a ML package for time series forecasting.
We have integrated the AutoARIMA model from this package.
This automatically tunes the classical Auto-regressive Integrated Moving Average ("ARIMA") forecasting algorithm.

Call this handler by
`USING ENGINE="statsforecast"` - you can see a full example in https://github.com/mindsdb/mindsdb/pull/4398

# Why is this integration useful? What does the ideal predictive use case for this integration look like? When would you definitely not use this integration?
StatsForecast uses classical methods, rather than deep learning, so models require little training time and are far less prone to overfitting.
The ideal use case is forecasting univariate time series, such as predicting the price each stock in an index like the S&P 500.
These models will also perform well in cases with short time-series, as they require little data to fit accurately relative to deep learning models.

Do not use this integration for non time-series data.

Other forecasting models, such as deep learning frameworks, may be more accurate for forecasting tasks with many exogenous features.

# Are models created with this integration fast and scalable, in general?
Model training is extremely fast, as these are classical methods. Training on the M1 stock price dataset, with 300k rows, completes in less than a minute.

# What are the recommended system specifications for models created with this framework?
N/A - model training is computationally light.

# To what degree can users control the underlying framework by passing parameters via the USING syntax?
The forecast horizon with the "horizon" arg.

The predictive model can be specified with the "model_name" arg. Users can choose between "AutoARIMA, AutoETS, AutoCES, AutoTheta". If no choice is made, the default is "AutoARIMA".

The data frequency can be specified with the "frequency" arg. If no frequency is specified, MindsDB tries to infer this automatically from the dataframe.

# Does this integration offer model explainability or insights via the DESCRIBE syntax?
Not implemented yet.

# Does this integration support fine-tuning pre-existing models (i.e. is the update() method implemented)? Are there any caveats?
Not needed - this is not a deep learning framework.

# Are there any other noteworthy aspects to this handler?
These models do not require training/testing/split because they have regularisation through Information Criterion, like Akaike (AIC) and Bayesian (BIC).
This allows model training and forecasting to take place simultaneously.

# Any directions for future work in subsequent versions of the handler?
Implement the DESCRIBE method.

# Please provide a minimal SQL example that uses this ML engine (pointers to integration tests in the PR also valid)
See integration test in https://github.com/mindsdb/mindsdb/pull/4398

An example of calling option USING args for "frequency" and "model_name" is at https://github.com/mindsdb/mindsdb/pull/4722

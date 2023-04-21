# Briefly describe what ML framework does this handler integrate to MindsDB, and how?
[Prophet](https://facebook.github.io/prophet/) is a forecasting procedure implemented in R and Python.
It is fast and provides completely automatic forecasts that can be tuned by hand by data scientists and analysts.

Call this handler by
`USING ENGINE="fbprophet"` - you can see a full example in [PR#5759](https://github.com/mindsdb/mindsdb/pull/5759)

# Why is this integration useful? What does the ideal predictive use case for this integration look like? When would you definitely not use this integration?
Prophet is very easy to use, and have useful features such as changepoint detection, holidays, decomposition of seasonality and trend, with no or minimal tuning.
It serves as a baseline model for many forecasting problems.

# Are models created with this integration fast and scalable, in general?
Prophet can only handle a single variate time series at a time. If a dataset contains multiple time series of different entities, e.g., sales for different products,
we need to build multiple Prophet models for each of the product. This is currently done in a sequential fashion. But further parallelism can be achieved by
implementing multiprocessing or using frameworks like `dask` or `ray`.

# What are the recommended system specifications for models created with this framework?
N/A - model training is computationally light if the number of time series is light to moderate.

# To what degree can users control the underlying framework by passing parameters via the USING syntax?
The forecast horizon with the "horizon" arg.
The data frequency can be specified with the "frequency" arg. If no frequency is specified, MindsDB tries to infer this automatically from the dataframe.

# Does this integration offer model explainability or insights via the DESCRIBE syntax?
This is currently not implemented. Prophet offers `cross_validation` to generate the performance report on the insample data. But this procedure is expensive.
It also provides decomposition of seasonality and trend. But since there are actually multiple models fitted (for different entities), how to easily output those info
is not trivial.

# Does this integration support fine-tuning pre-existing models (i.e. is the update() method implemented)? Are there any caveats?
Not needed - this is not a deep learning framework.

# Are there any other noteworthy aspects to this handler?
NA

# Any directions for future work in subsequent versions of the handler?
- Visualisations of forecasts would help for quickly sense-checking results.
- `DESCRIBE model` can be enhanced to output meaningful information.
- Parallelism can be implemented to speed up the model fitting process.


# Please provide a minimal SQL example that uses this ML engine (pointers to integration tests in the PR also valid)
See integration test in [PR#5759](https://github.com/mindsdb/mindsdb/pull/5759)

# Briefly describe what ML framework does this handler integrate to MindsDB, and how?
Nixtla is an ML package for time series forecasting.

It provides access to zero-shot forecasting models, meaning they do not require training to provide predictions.

These models can be finetuned to work better on any specific domain.

User requires a Nixtla API key to use this handler.

Call this handler by
`USING ENGINE="nixtla"`.

# Why is this integration useful? What does the ideal predictive use case for this integration look like? When would you definitely not use this integration?

# Are models created with this integration fast and scalable, in general?

# What are the recommended system specifications for models created with this framework?
N/A - no model training or inference is done on premise.

# To what degree can users control the underlying framework by passing parameters via the USING syntax?
The forecast horizon with the "horizon" arg.

The predictive model can be specified with the "model_name" arg. Possible options are ?.

If no choice is made, the default is ?.

The data frequency can be specified with the "frequency" arg. If no frequency is specified, MindsDB tries to infer this automatically from the dataframe.

# Does this integration offer model explainability or insights via the DESCRIBE syntax?
We provide ?

# Does this integration support fine-tuning pre-existing models (i.e. is the update() method implemented)? Are there any caveats?
Yes. The update() method is implemented.

# Are there any other noteworthy aspects to this handler?

# Any directions for future work in subsequent versions of the handler?

# Please provide a minimal SQL example that uses this ML engine (pointers to integration tests in the PR also valid)

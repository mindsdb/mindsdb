# Briefly describe what ML framework does this handler integrate to MindsDB, and how?
AutoKeras is an AutoML package for both regression and classification tasks.
We have integrated both the Regressor and Classifier models from this package.
Our implementation will automatically determine whether the task is regression or classification, the user does not need to specify this.

Call this handler with
`USING ENGINE='autokeras'` - you can see a full example in https://github.com/mindsdb/mindsdb/pull/4559

# Why is this integration useful? What does the ideal predictive use case for this integration look like? When would you definitely not use this integration?
AutoKeras will build an accurate deep learning model for the end-user, with no prior knowledge of deep learning required.
The handler will automatically search and tune different neural network architectures to find the most accurate for the given problem.
The ideal use case is a very large dataset with many predictive features, where the user doesn't have strong priors about how these features may affect the target variable.

Do not use this integration for for small datasets, as neural networks are prone to overfitting.

Do not use this integration if you need to fit a model very quickly, as training time can be long.

# Are models created with this integration fast and scalable, in general?
Making predictions with these models are very fast.

However, the model search and auto-training process is slow because AutoKeras uses neural networks. Using the default settings, model training may take several hours. We provide an optional setting to reduce training time (see below).


# What are the recommended system specifications for models created with this framework?
We recommend training this handler on a machine with a CUDA-enabled GPU.
We would also recommend training on a remote server, or a machine you can leave running, given potentially long training times.

# To what degree can users control the underlying framework by passing parameters via the USING syntax?
We provide an optional argument,

```
USING
    ENGINE='autokeras',
    train_time={x}
```

where x can take values from 0 to 1 (default to 1). Lower values will reduce training time linearly e.g. a value of 0.1 will cut training time to 10% of the default. This comes at the cost of accuracy, as the neural net model search space is reduced by the same factor.

# Does this integration offer model explainability or insights via the DESCRIBE syntax?
Not implemented yet.

# Does this integration support fine-tuning pre-existing models (i.e. is the update() method implemented)? Are there any caveats?
Not implemented yet.

# Are there any other noteworthy aspects to this handler?
AutoKeras automatically splits the data into training and validation sets, the user does not need to do this.
The original Keras library was for computer vision, so this would be a good option if users want to do image analysis.

Users should set up a Conda environment to use this handler, as Keras depends on Tensorflow rather than PyTorch.
They should follow the instructions at:
1. https://www.tensorflow.org/install/pip
2. https://autokeras.com/install/

# Any directions for future work in subsequent versions of the handler?
Implement the DESCRIBE and UPDATE methods.

If there is user demand for image analysis, implement the image models from AutoKeras.

# Please provide a minimal SQL example that uses this ML engine (pointers to integration tests in the PR also valid)
See integration test in https://github.com/mindsdb/mindsdb/pull/4559

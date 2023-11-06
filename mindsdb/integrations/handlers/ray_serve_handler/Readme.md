# RayServeHandler 
This is the implementation of the RayServe for MindsDB.

## RayServeHandler 

The `RayServeHandler` is a machine learning engine that integrates with Ray Serve, a powerful Python library for building and deploying machine learning models as HTTP microservices. More information about this model can be found (here)[https://docs.ray.io/en/latest/serve/index.html].

## Prerequisites

To get started with Ray Serve, first, install Ray and its dependencies, including Ray Serve, using pip:

```bash
pip install "ray[serve]"
```

Before using the `RayServeHandler`, make sure you have the following prerequisites in place:

- Ray Serve server should be running, as the engine relies on it to serve and deploy machine learning models as microservices.

## Implementation

The `RayServeHandler` is implemented using Python and relies on the Ray Serve library for creating and deploying machine learning models. Here are the key implementation details:

- The package provides a Python class called `RayServeHandler`, which encapsulates the functionality required to create and use machine learning models with Ray Serve.

- It establishes a connection with a running Ray Serve server, making it essential to have a functioning Ray Serve setup.

- The handler offers methods for model creation, prediction, and model description.

- Error handling is integrated to ensure a smooth experience. The package validates input parameters, such as training and prediction URLs, and raises appropriate exceptions if issues are encountered.

## Usage

### Creating the Model

To create a machine learning model with the `RayServeHandler`, follow these steps:

1. Initialize the `RayServeHandler`.

2. Specify the target column for training.

3. Provide the training data as a DataFrame (optional).

4. Define the parameters for training using the 'args' parameter.

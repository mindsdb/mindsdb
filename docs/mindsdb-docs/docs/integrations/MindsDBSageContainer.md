---
id: mindsdb-container
title: MindsDB container
---

The general flow is to train and deploy models within SageMaker, create endpoints and take advantage of automated machine learning with MindsDB.

## The mindsdb_impl Code Structure
All of the components we need to package MindsDB for Amazon SageMager are located inside the mindsdb_impl directory:
```
|-- Dockerfile
|-- build_and_push.sh
`-- mindsdb_impl
    |-- nginx.conf
    |-- predictor.py
    |-- serve
    |-- train
    `-- wsgi.py
```
All of the files that will be packaged in the container working directory are inside mindsdb_impl:
* nginx.conf - is the configuration file for the nginx .
* predictor.py is the program that actually implements the Flask web server and the MindsDB predictions for this app. We have modified this to use the MindsDB Predictor and to accept different types of tabular data for predictions.
* serve is the program that is started when the container is started for hosting.
* train is the program that is invoked when the container is being run for training. We have modified this program to use MindsDB Predictor interface.
* wsgi.py is a small wrapper used to invoke the Flask app. 

## Build Docker Image

The docker command for building the image is build and -t parameter provides the name of the image:

```sh
docker build -t mindsdb-impl .
```

After getting the `Successfully built` message, we should be able to list the image by running the list command:

```sh
docker image list
```

## Test the Container Locally

The local_test directory contains all of the scripts and data samples for testing the built container on the local machine.

* train_local.sh: Instantiate the container configured for training.
* serve_local.sh: Instantiate the container configured for serving.
* predict.sh: Run predictions against a locally instantiated server.
* test-dir:  This directory is mounted in the container.
* test_data: This directory contains a few tabular format datasets used for getting the predictions.
* input/data/training/file.csv`: The training data.
* model: The directory where MindsDB writes the model files.
* call.py: This cli script can be used for testing the deployed model on the SageMaker endpoint

To train the model execute:
```
./train_local.sh mindsdb-impl
```
Next, start the inference server that will provide an endpoint for getting the predictions. Inside the local_test directory execute serve_local.sh script:
```
./serve_local.sh mindsdb-impl
```
To run predictions against the invocations endpoint, use predict.sh script:
```
./predict.sh test_data/diabetest-test.csv text/csv
```
The arguments sent to the script are the test data and content type of the data.

## Deploy the Image on Amazon ECR (Elastic Container Repository)

To push the image use build-and-push.sh script. Note that the script will look for an AWS EC Repository in the default region that you are using, and create a new one if that doesn't exist.

```
./build-and-push.sh mindsdb-impl
```

After the mindsdb-impl image is deployed to Amazon ECR, you can use it inside SageMaker.
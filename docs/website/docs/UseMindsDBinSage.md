---
id: use-mindsdb-in-sage
title: Train and host MindsDB models
---

The following section explaines how to train and host models using [Amazon SageMaker console](https://console.aws.amazon.com/sagemaker/).

## Create Train Job

Follow the steps below to successfully start a train job and use MindsDB to create the models:
1. Open the Amazon SageMaker console at https://console.aws.amazon.com/sagemaker/.
2. From the left panel choose Create Training Job and provide the following information
    * Job name
    * IAM role - it’s best if you provide AmazonSageMakeFullAccess IAM policy
    * Algorithm source - Your own algorithm container in ECR
3. Provide container ECR path
    * Container - the ECR registry Image URI that we have pushed 
    * Input mode - File
4. Resource configuration - leave the default instance type and count
5. Hyperparameters - MindsDB requires to_predict column name, so it knows which column we want to predict, e.g.
    * Key - to_predict
    * Value - Class(the column in diabetes dataset) 
6. Input data configuration
    * Channel name - training
    * Data source - s3
    * S3 location - path to the s3 bucket where the dataset is located

7.Output data configuration -  path to the s3 where the models will be saved

## Model creation
 Create model and add the required settings:
1. Model name - must be unique
2. IAM role - it’s best if you provide AmazonSageMakeFullAccess IAM policy
3. Container input options
    * Provide model artifacts and inference image location
    * Use a single model
    * Location of the inference code - 846763053924.dkr.ecr.us-east-1.amazonaws.com/mindsdb_impl:latest
    * Location of model artifacts - path to model.tar.gz inside s3 bucket.

## Endpoint configuration

In the endpoint configuration, provide which models to deploy, and the hardware requirements:

1. Endpoint configuration name
2. Add model - select the previously created model
3. Choose Create endpoint configuration.

## Create endpoint

The last step is to create endpoint and provide endpoint configuration that specify which models to deploy and the requirements:

1. Endpoint name
2. Attach endpoint configuration - select the previously created endpoint configuration
3. Choose Create endpoint.

After finishing the above steps, SageMaker will create a new instance and start the inference code.
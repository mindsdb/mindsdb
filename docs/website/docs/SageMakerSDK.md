---
id: use-sage-sdk
title: Train and host MindsDB Models using SageMaker SDK
---

The following section explaines how to train and host models using [Amazon SageMaker SDK](https://sagemaker.readthedocs.io/en/stable/).

### Add dependencies
Install SageMaker SDK :
```
pip install sagemaker
```
First, add IAM Role that have AmazonSageMakerFullAccess Policy.
```python
import sagemaker as sage
role = "arn:aws:iam::123213143532:role/service-role/AmazonSageMaker-ExecutionRole-20199"
sess = sage.Session()
account = sess.boto_session.client('sts').get_caller_identity()['Account']
```
Next, provide s3 bucket where the models will be saved, get aws region from session and add URI to MindsDB image in AWS ECR:
```python
bucket_path = "s3://mdb-sagemaker/models/"
region = sess.boto_session.region_name
image = '{}.dkr.ecr.{}.amazonaws.com/mindsdb_lts:latest'.format(account, region)
``` 
### Start training
The required properties for invoking SageMaker training using Estimator are:
* The image name(str). The MindsDB container URI on ECR
* The role(str). AWS arn with SageMaker execution role
* The instance count(int). The number of machines to use for training.
* The instance type(str). The type of machine to use for training.
* The output path(str). Path to the s3 bucket where the model artifact will be saved.
* The session(sagemaker.session.Session). The SageMaker session object that we’ve defined in the code above.
* The base job name(str). The name of the training job
* The hyperparameters(dict). The MindsDB container requires to_predict value so it knows which column to predict.

```python
mindsdb_impl = sage.estimator.Estimator(image,
                       role, 1, 'ml.m4.xlarge',
                       output_path=bucket_path,
                       sagemaker_session=sess,
                       base_job_name="mindsdb-lts-sdk",
                       hyperparameters={"to_predict": "Class"})
```

### Deploy the model
The required configuration for deploying model:
* initial_instance_count (int) – The initial number of instances to run in the Endpoint created from this Model.
* instance_type (str) – The EC2 instance type to deploy this Model to. For example, ‘ml.p2.xlarge’, or ‘local’ for local mode.
* endpoint_name (str) - The name of the endpoint on SageMaker.

```python
dataset_location = 's3://mdb-sagemaker/diabetes.csv'
mindsdb_impl.fit(dataset_location)
predictor = mindsdb_impl.deploy(1, 'ml.m4.xlarge', endpoint_name='mindsdb-impl')
 ```

### Make a prediction

Load the test dataset and then call the Predictor predict method with test data.
```python
with open('test_data/diabetes-test.csv', 'r') as reader:
        when_data = reader.read()
print(predictor.predict(when_data).decode('utf-8'))
```
### Delete the endpoint

Don't forget to delete the endpoint after using it.
```python
sess.delete_endpoint('mindsdb-impl')
```

### Full code example

```python
import sagemaker as sage

# Add AmazonSageMaker Execution role here
role = "arn:aws:iam:"

sess = sage.Session()
account = sess.boto_session.client('sts').get_caller_identity()['Account']
bucket_path = "s3://mdb-sagemaker/models/"
region = sess.boto_session.region_name
image = '{}.dkr.ecr.{}.amazonaws.com/mindsdb_lts:latest'.format(account, region)

#Hyperparameters to_predict is required for MindsDB container
mindsdb_impl = sage.estimator.Estimator(image,
                       role, 1, 'ml.m4.xlarge',
                       output_path=bucket_path,
                       sagemaker_session=sess,
                       base_job_name="mindsdb-lts-sdk",
                       hyperparameters={"to_predict": "Class"})

dataset_location = 's3://mdb-sagemaker/diabetes.csv'
mindsdb_impl.fit(dataset_location)
predictor = mindsdb_impl.deploy(1, 'ml.m4.xlarge', endpoint_name='mindsdb-impl')
with open('test_data/diabetes-test.csv', 'r') as reader:
        when_data = reader.read()
print(predictor.predict(when_data).decode('utf-8'))
```


---
id: call-sage-endpoint
title: Call SageMaker Endpoint
---

To call the SageMaker endpint you can create Jupyter Notebook or use call.py script.

## Jupyter Notebook

```python
import boto3

endpointName = 'mindsdb-impl'

# load test dataset
with open('diabetest-test.csv', 'r') as reader:
        payload = reader.read()

# Talk to SageMaker
client = boto3.client('sagemaker-runtime')
response = client.invoke_endpoint(
    EndpointName=endpointName,
    Body=payload,
    ContentType=text/csv,
    Accept='Accept'
)

print(response['Body'].read().decode('ascii'))

```
Run the code and you should see the prediction response from the endpoint:
```json
{
 "prediction": "* We are 96% confident the value of "Class" is positive.", 
 "class_confidence": [0.964147493532568]
}
```

## [call.py](https://github.com/mindsdb/mindsdb-sagemaker-container/blob/master/local_test/call.py) Script

The required arguments are:
* endpoint - the name of the SageMaker endpoint
* dataset - the location of test dataset
* content type - mime type of the data


```python
python3 call.py --endpoint mindsdb-impl --dataset test_data/diabetes-test.json --content-type application/json
```

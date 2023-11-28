## SNS Handler

This is the implementation of the SNS handler for MindsDB.

### About SNS

Amazon Simple Notification Service (Amazon SNS) is a managed service that provides message delivery from publishers to subscribers (also known as producers and consumers). Publishers communicate asynchronously with subscribers by sending messages to a topic, which is a logical access point and communication channel. Clients can subscribe to the SNS topic and receive published messages using a supported endpoint type, such as Amazon Kinesis Data Firehose, Amazon SQS, AWS Lambda, HTTP, email, mobile push notifications, and mobile text messages (SMS). <br>
https://docs.aws.amazon.com/sns/latest/dg/welcome.html

### Handler Implementation

This handler was implemented using the `boto3`, the AWS SDK for Python.
The required arguments to establish a connection are,
* `aws_access_key_id`: the AWS access key that identifies the user or IAM role
* `aws_secret_access_key`: the AWS secret access key that identifies the user or IAM role
* `region_name`: the AWS region

### Usage

In order to make use of this handler and connect to SNS in MindsDB, the following syntax can be used,


```sql
CREATE DATABASE sns
WITH
    engine = 'sns',
    parameters = {
      "aws_access_key_id": "aws_access_key_id_value",
      "aws_secret_access_key": "aws_secret_access_key_value",
      "region_name": "region_name"
    };
```
Now, you can view the topics list 
```sql


```


 mysql -u mindsdb -h 127.0.0.1  -P 47335
### Implemented Features
- [x] Feature A
- [x] Feature B
  - [x] Feature B1
  - [ ] Feature B2

### TODOs
- [ ] Feature C
- [ ] Feature D

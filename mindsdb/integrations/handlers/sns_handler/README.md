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
Now, you can create a topic 
```sql
 insert into topics(name) values('test');

```

then view topic lists
```sql
 select * from topics;
```

Each topic should look like this:
+-----------------------------------------+
| topic_arn                               |
+-----------------------------------------+
| arn:aws:sns:us-east-1:000000000000:test |
+-----------------------------------------+

Then we can publish a single message
```sql
insert into messages(message,topic_arn) values('some_message','arn:aws:sns:us-east-1:000000000000:test')
```
we can publish messages using the publish batch functionality

```sql
insert into messages(message,topic_arn,id,subject,message_deduplication_id,message_group_id) values('some_message','arn:aws:sns:us-east-1:000000000000:test','333','aaaa','000-222','ssss'),('some_message','arn:aws:sns:us-east-1:000000000000:test','3373','aaaa','000-222','ssss')
```



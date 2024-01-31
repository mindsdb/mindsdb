import boto3
import pprint
client = boto3.client(
           'sns',
            aws_access_key_id = "test",
            aws_secret_access_key = "test",
            #aws_access_key_id = "AKIAU6EY6M3JAIRJLUNK",
            #aws_secret_access_key = "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",
            endpoint_url = "http://localhost:4566",
            #verify=False
            )

#client.create_topic(Name="test_topic")
#print(client.list_topics())
#client.delete_topic(TopicArn='arn:aws:sns:us-east-1:339623175890:aaaaa')
#response = client.delete_topic(TopicArn='arn:aws:sns:us-east-1:339623175890:aaaaa')
#print("out============="+str(response))
#print(str(response['ResponseMetadata']['HTTPStatusCode']))
#pp = pprint.PrettyPrinter(indent=4)
#pp.pprint(client.publish_message({"topic_arn": "arn:aws:sns:us-east-1:000000000000:test", "message": "Test_message"}))
#print(str(client.publish( TopicArn="arn:aws:sns:us-east-1:000000000000:aaaa" , Message = "qwerty")))

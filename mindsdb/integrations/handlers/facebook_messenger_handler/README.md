# Build your own Facebook Messenger AI agent

Imagine you want to engage with your followers on Facebook Messenger and answer all their questions promptly. Thanks to MindsDB, you can train an AI system that helps you manage automation on Facebook Messenger responses.

For this particular example, we would like to automatically respond to the people that message us as follows:

If the message is positive, write a short thank you note.
If the message is negative or the message has a question, invite them to our slack channel.
We will build our Facebook Messenger AI tool in a few SQL commands in MindsDB.

Connecting to Facebook Messenger
To do that, you can follow these steps to obtain a PAGE ACCESS TOKEN from Facebook.

```
CREATE DATABASE my_facebook_messenger 
WITH 
    ENGINE = 'facebook_messenger',
    PARAMETERS = {
      "page_access_token": "facebook page access TOKEN"
    };
```
This creates a database called my_facebook_messenger. This database ships with a table called messages that we can use to search for messages as well as to write messages.

Searching for messages in SQL
Let's get a list of messages that contain the word mindsdb

```
SELECT 
   id, created_at, author_name, text 
FROM my_facebook_messenger.messages 
WHERE 
   text LIKE '%mindsdb%' 
   AND created_at > '2023-02-16' 
LIMIT 20;
```

Writing messages using SQL
Let's test by sending a few things.

```
INSERT INTO my_facebook_messenger.messages (recipient_id, text)
VALUES 
    ('1626198053446369280', 'MindsDB is great! now its super simple to build ML powered apps'),
    ('1626198053446369280', 'Holy!! MindsDB is the best thing they have invented for developers doing ML');
```
Those messages should be live now on Facebook Messenger, like magic, right?

Let's use AI to write responses for us
To do this, we would like to create a machine-learning model that can write responses. We will be using OpenAI GPT-3 for this. The way it works is that we will create a model that can take a prompt and give a message based on that prompt.

The query looks like:

```
CREATE MODEL mindsdb.facebook_messenger_response_model                           
PREDICT response
USING
  engine = 'openai', 
  max_tokens = 200,             
  prompt_template = 'from message "{{text}}" by "{{name}}", if their comment is a question, invite them to join the MindsDB slack using this link http://bitly.com/abc. Otherwise, simply write a thank you message';```

```
This created a virtual AI table called facebook_messenger_response_model. We can query this model as if it was a table, but it will generate responses for us, as follows:

```

SELECT response FROM mindsdb.facebook_messenger_response_model 
WHERE author_name = 'xyz' and text = 'I love this, can I learn more?';

```
Schedule a job
Finally, we can now automate the responses by writing a job that:

Checks for new messages
Generates a response using the OpenAI model
Sends the responses back
All this in one SQL command:


```
CREATE JOB auto_respond AS (

 INSERT INTO my_facebook_messenger.messages (recipient_id, text)
 SELECT 
   t.id AS recipient_id, 
   r.response AS text
 FROM my_facebook_messenger.messages t
 JOIN mindsdb.facebook_messenger_response_model r 
      WHERE 
      t.text LIKE '%mindsdb%'
      AND t.created_at > "{{PREVIOUS_START_DATETIME}}"
  limit 2
)
EVERY HOUR

```



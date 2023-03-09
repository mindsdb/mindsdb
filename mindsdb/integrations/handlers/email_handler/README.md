# Build your own email AI agent

You can start by creating a database as follows

```
CREATE DATABASE mailbox
USING 
    engine="email_handler"
    imap_server="someserver"
    smtp_server="someserver"
    username="someusername"
    password="somepassword"
```

now we can query emails as tables:

```
SELECT id, from, to, subject, body FROM mailbox.emails WHERE folder = 'inbox' AND date > 'some date' 
```

You can create a GPT model to manage responses

```
CREATE MODEL mindsdb.gpt3_model
PREDICT generated_answer
USING
  ENGINE='openai',
  max_tokens = 200
```

You could write and respond to emails, for example using gpt to generate a response:

```
INSERT INTO mailbox.emails (reply_to_id, to, body) 
SELECT e.id AS reply_to_id, 'all', m.generated_answer AS body
FROM mailbox.emails e JOIN mindsdb.gpt3_model m
WHERE 
     m.prompt = "if message doesn't feel like a request for a meeting, return word PASS Otherwise, Write a thank you email response asking for the reason they would like to meet, unless the message already contains a reason and the reason is about an issue with the mindsdb product or questions about how to use mindsdb in production, then invite them to schedule a calendly http://someurl, try to address it to the person's name if you can extract name from {{e.from}}"
     AND m.generated_aswer != 'PASS';
```
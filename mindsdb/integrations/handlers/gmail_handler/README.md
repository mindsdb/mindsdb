# Gmail API Integration

This handler integrates with the [Gmail API](https://developers.google.com/gmail/api/guides/overview)
to make emails data available to use for model training and automate email responses.

## Example: Automate your Email response

To see how the Gmail handler is used, let's walk through the steps to create a simple model which will let us respond to incoming email automatically.

## Connect to the Gmail API

To use the Gmail API we need to setup a Google Cloud Project and a Google Account with Gmail enabled.

Before proceeding further, we will need to enable the Gmail API from the Google Cloud Console.

We will also need to create OAuth Client Ids for authenticating users, and possibly an Auth Consent Screen (if this is the first time we're setting up OAuth)

Setting up OAuth Client Id will give us a credentials file which we will need in our mindsdb setup. You can find more information on how to do
this [here](https://developers.google.com/gmail/quickstart/python).

**Optional:**  The credentials file can be stored in the gmail_handler folder in
the `mindsdb/integrations/handlers/gmail_handler` directory.

~~~~sql
CREATE DATABASE mindsdb_gmail
WITH ENGINE = 'gmail',
parameters = {
    "credentials_file": "mindsdb/integrations/handlers/gmail_handler/credentials.json",
    -- "scopes": "['SCOPE_1', 'SCOPE_2', ...]" -- Optional scopes. By default 'https://.../gmail.compose' & 'https://.../gmail.readonly' scopes are used
};    
~~~~

This creates a database called mindsdb_gmail. This database ships with a table called emails that we can use to search for
emails as well as to write emails.

## Searching for emails

Let's get a list of emails from our mailbox. The query supports all the search term we can use with gmail. For more details please check [this link](https://support.google.com/mail/answer/7190)

~~~~sql
SELECT *
FROM mindsdb_gmail.emails
WHERE query = 'from:test@example.com OR search_text OR from:test@example1.com'
AND label_ids = "INBOX,UNREAD" 
LIMIT 20;
~~~~

## Writing Emails

To write email we need to pass the destination email address, email subject and email body text. When replying to an email if we want to create email threads, then we must also pass the thread_id as well as the message_id to which we're replying.

~~~~sql
INSERT INTO mindsdb_gmail.emails (thread_id, message_id, to_email, subject, body)
VALUES ('187cbdd861350934d', '8e54ccfd-abd0-756b-a12e-f7bc95ebc75b@Spark', 'test@example2.com', 'Trying out MindsDB',
        'This seems awesome. You must try it out whenever you can.')

~~~~

## Creating a model to automate email replies

Now that we know how to pull emails into our database as well as how to write emails, we can make use of OpenAPI or other AI APIs to write replies for us. For example, the below creates a model using the OpenAI's `gpt-3.5-turbo` model. 

~~~~sql
CREATE MODEL mindsdb.gpt_model
PREDICT response
USING
engine = 'openai',
max_tokens = 500,
api_key = 'your_api_key', 
model_name = 'gpt-3.5-turbo',
prompt_template = 'From input message: {{input_text}}\
by from_user: {{from_email}}\
In less than 500 characters, write an email response to {{from_email}} in the following format:\
Start with proper salutation and respond with a short message in a casual tone, and sign the email with my name mindsdb';
~~~~

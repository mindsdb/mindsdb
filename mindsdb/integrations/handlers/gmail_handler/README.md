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
    -- "scopes": ['SCOPE_1', 'SCOPE_2', ...] -- Optional scopes. By default 'https://.../gmail.compose' & 'https://.../gmail.readonly' scopes are used
};    
~~~~

This creates a database called mindsdb_gmail. This database ships with a table called emails that we can use to search for
emails as well as to write emails.

You can also create a database by giving the credentials file from a s3 signed url.To do this you need to pass in the credentials_file parameter as a signed url.For example:
~~~~sql
CREATE DATABASE mindsdb_gmail
WITH ENGINE = 'gmail',
parameters = {
    "credentials_file": "https://s3.amazonaws.com/your_bucket/credentials.json?AWSAccessKeyId=your_access_key&Expires=your_expiry&Signature=your_signature",
    -- "scopes": ['SCOPE_1', 'SCOPE_2', ...] -- Optional scopes. By default 'https://.../gmail.compose' & 'https://.../gmail.readonly' scopes are used
};
~~~~



## Searching for emails

Let's get a list of emails from our mailbox using the `SELECT` query.

~~~~sql
SELECT *
FROM mindsdb_gmail.emails
WHERE query = 'alert from:*@google.com'
AND label_ids = "INBOX,UNREAD"
LIMIT 20;
~~~~
This will search your Gmail inbox for any email which contains the text `alert` and is from `google.com` domain (notice the use of the wildcard `*`).

The returned result should have ROWs like this

| id | message_id | thread_id | label_ids | sender | to | date | subject | snippet | body |
| ----------- | ----------- | ----------- | ----------- | ----------- | ----------- | ----------- | ----------- | ----------- | ----------- |
| 187d3420d1a8690a     | <oXgFXqq1yB7x2OpCf0cBaw@notifications.google.com> | 187d3420d1a8690a | ["UNREAD","CATEGORY_UPDATES","INBOX"] | "Google" <no-reply@accounts.google.com> | test@gmail.com  | Sun, 30 Apr 2023 17:42:12 GMT | Security alert | Application was granted access to your Google Account test@gmail.com If you did not grant access, you should check this activity and secure your account. Check activity You can also see | [image: Google] Application was granted access to your Google Account test@gmail.com If you did not grant access, you should check this activity and secure your account. Check activity... |

where
* query - The search term. The query parameter supports all the search terms we can use with gmail. For more details please check [this link](https://support.google.com/mail/answer/7190)
* label_ids - A comma separated string of labels to search for. E.g. "INBOX,UNREAD" will search for unread emails in inbox, "SENT" will search for emails in the sent folder.
* include_spam_trash - BOOLEAN (TRUE / FALSE). By default it is FALSE. If included, the search will cover the SPAM and TRASH folders.

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
prompt_template = 'From input message: {{body}}\
by from_user: {{sender}}\
In less than 500 characters, write an email response to {{sender}} in the following format:\
Start with proper salutation and respond with a short message in a casual tone, and sign the email with my name mindsdb';
~~~~

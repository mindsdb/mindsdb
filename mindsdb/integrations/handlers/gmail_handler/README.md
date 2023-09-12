# Gmail API Integration

This handler integrates with the [Gmail API](https://developers.google.com/gmail/api/guides/overview)
to make emails data available to use for model training and automate email responses.

## Example: Automate your Email response

To see how the Gmail handler is used, let's walk through the steps to create a simple model which will let us respond to incoming email automatically.

## Connect to the Gmail API

To use the Gmail API we need to set up a Google Cloud Project and a Google Account with Gmail enabled.

Before proceeding further, we will need to enable the Gmail API from the Google Cloud Console.

We will also need to create OAuth Client Ids for authenticating users, and possibly an Auth Consent Screen (if this is the first time we're setting up OAuth).

Setting up OAuth Client Id will give us a credentials file which we will need in our mindsdb setup. You can find more information on how to do
this [here](https://developers.google.com/gmail/api/quickstart/python).

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

You can also create a database by giving the credentials file from a s3 pre signed url. To do this you need to pass in the credentials_file parameter as a signed url. For example:

~~~~sql
CREATE DATABASE mindsdb_gmail
WITH ENGINE = 'gmail',
parameters = {
    "credentials_url": "https://s3.amazonaws.com/your_bucket/credentials.json?AWSAccessKeyId=your_access_key&Expires=your_expiry&Signature=your_signature",
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

The returned result should have ROWs like this,

| id | message_id | thread_id | label_ids | sender | to | date | subject | snippet | history_id | size_estimate | body | attachments |
| ----------- | ----------- | ----------- | ----------- | ----------- | ----------- | ----------- | ----------- | ----------- | ----------- | ----------- | ----------- | ----------- |
| 187d3420d1a8690a     | <oXgFXqq1yB7x2OpCf0cBaw@notifications.google.com> | 187d3420d1a8690a | ["UNREAD","CATEGORY_UPDATES","INBOX"] | "Google" <no-reply@accounts.google.com> | test@gmail.com | Sun, 30 Apr 2023 17:42:12 GMT | Security alert | Application was granted access to your Google Account test@gmail.com If you did not grant access, you should check this activity and secure your account. Check activity You can also see | 232290 | 200854 | [image: Google] Application was granted access to your Google Account test@gmail.com If you did not grant access, you should check this activity and secure your account. Check activity... | [{"filename": "test.pdf", "mimeType": "application/pdf", "attachmentId": "ANGjdJ_V7MKXakDKYhP3rHPsEE72qHtXXBqseBeXJje2kJK-ksm-h9NtDQxnO1R_1FS6e2H6BqryLQS0q2-nEN3jpnUHQXjeMSJ4-HtYQcDoyJk3-e5eBW64-mnlqajKTxMWPKkGjD1Gs99-EYHC_hrTDI_N09hXkKWAgrS5BNLjI1azMo5eA"}, {"filename": "test.doc", "mimeType": "application/msword", "attachmentId": "ANGjdJ9iw-cJls_xTfX7bXMdHjmNp3aP9fiFjKKjvnJPKJijolW8Mv-H4-tCRyuA8xOktd8KMbqfxwVmM68TPxkwMq4YOEV3sHoVoBPUoyAWK-CpRFhnFaZu9CJpF264nVYJv7Kqz52qgzkGHqvdBR82WWMfGZxP8XLp6_EYcyVvFdOFHzZc30QJb"}] |

where
* query - The search term. The query parameter supports all the search terms we can use with gmail. For more details please check [this link](https://support.google.com/mail/answer/7190)
* label_ids - A comma separated string of labels to search for. E.g. "INBOX,UNREAD" will search for unread emails in inbox, "SENT" will search for emails in the sent folder.
* include_spam_trash - BOOLEAN (TRUE / FALSE). By default it is FALSE. If included, the search will cover the SPAM and TRASH folders.
* include_attachments - BOOLEAN (TRUE / FALSE). By default it is FALSE. If included, the search will include emails with attachments.


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

## Find spam emails
You can check if an email is spam or not by using the pretrained model of hugging face. To do this, you can use the following query:
* First you have to create a model:
~~~~sql
CREATE MODEL mindsdb.spam_classifier                           
PREDICT PRED                           
USING
  engine = 'huggingface',              
  task = 'text-classification',        
  model_name = 'mrm8488/bert-tiny-finetuned-sms-spam-detection', 
  input_column = 'text_spammy',        
  labels = ['ham', 'spam'];
~~~~
* Then you can have to create a view of the email table that contains the snippet or the body of the email. For example by using the snippet:
~~~~sql
CREATE VIEW mindsdb.emails_text AS(
    SELECT snippet AS text_spammy
    FROM mindsdb_gmail.emails
)
~~~~
* Finally, you can use the model to predict if an email is spam or not:
~~~~sql
SELECT h.PRED, h.PRED_explain, t.text_spammy AS input_text
FROM mindsdb.emails_text AS t
JOIN mindsdb.spam_classifier AS h;
~~~~

## Find the email sentiment
First create the model to find the sentiment of the email:
~~~~sql
CREATE MODEL email_sentiment_classifier
PREDICT sentiment
USING engine='huggingface',
  model_name= 'cardiffnlp/twitter-roberta-base-sentiment',
  input_column = 'email',
  labels=['negative','neutral','positive'];
~~~~

Then create a view of the email table that contains the snippet or the body of the email.For example by using the snippet:
~~~~sql
CREATE VIEW mindsdb.emails_text AS(
    SELECT snippet AS email
    FROM mindsdb_gmail.emails
)

~~~~
Finally, you can use the model to predict the sentiment of the email:
~~~~sql
SELECT input.email , model.sentiment
FROM  mindsdb.emails_text AS input
JOIN email_sentiment_classifier AS model;
~~~~

## Delete emails
You can delete emails by using the following query:
~~~~sql
DELETE FROM mindsdb_gmail.emails
WHERE message_id = '187cbdd861350934d';
~~~~

# Update email labels
You can update the labels of an email by using the following query:
~~~~sql
UPDATE mindsdb_gmail.emails
set addLabel="SPAM",removeLabel = "UNREAD"
WHERE message_id = '187cbdd861350934d';
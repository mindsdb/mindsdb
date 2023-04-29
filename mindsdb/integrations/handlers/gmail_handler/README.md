# Gmail API Integration

This handler integrates with the [Gmail API](https://developers.google.com/gmail/api/guides)
to make gmail data available to use for model training and predictions.

## Example: Write automated emails

To see how the Gmail handler is used, let's walk through a simple example to create a model to predict

## Connect to the Gmail API
## Prequisites
*  You will need to have a Google account and have enabled the Gmail API.
*  A project in the [Google Cloud Console](https://console.cloud.google.com/) with Gmail Api enabled.
* A credentials file for the project. You can find more information on how to do this [here](https://developers.google.com/workspace/guides/create-credentials).

We start by creating a database to connect to the Gmail API. In order to do this,as said before, you will need to obtain credentials:


**Optional:**  The credentials file can be stored in the gmail_handler folder in
the [mindsdb/integrations/gmail_handler](mindsdb/integrations/handlers/gmail_handler) directory.

~~~~sql
CREATE
DATABASE  gmail_test
WITH  ENGINE = 'gmail',
parameters = {
    "path_to_credentials_file": "/home/marios/PycharmProjects/mindsdb/mindsdb/integrations/handlers/gmail_handler/credentials.json"
}   
~~~~

This creates a database called gmail_test. This database comes with a table called emails that we can use to search for emails as well as to process emails.

## Searching for emails with gmail search operators

You can use the gmail search operators to search for emails. For example, to get a list of emails from a specific sender(e.g. me), you can use the following query:

~~~~sql
SELECT *
FROM gmail_test.emails
Where q = "from:me"
~~~~

You can also write more complex queries. For example, to get a list of emails that are send after a specific day, you can use the following query:
~~~~sql
SELECT *
FROM gmail_test.emails
Where q = "after:2022/05/03"
~~~~

The possibilities of searching and querying are endless. You can find more information on how to do this [here](https://support.google.com/mail/answer/7190?hl=en).


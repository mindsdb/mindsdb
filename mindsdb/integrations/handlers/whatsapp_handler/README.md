# WhatsApp Handler

Whatsapp handler for MindsDB facilitates the ability to send messages to your Whatsapp using Twilio Whastapp APIs and retrieve the conversation history.

---

## About Whatsapp

WhatsApp is a popular messaging application that allows users to send text messages, voice messages, make voice and video calls, share media files, and more.

## Whatsapp Handler Implementation

This handler was implemented using [Twilio Python SDK](https://www.twilio.com/docs/libraries/python). This SDK provides a very effective way to integrate with the Twilio Whatapp API.

## Whatsapp Handler Initialization

The Whatsapp handler is initialized with the following parameters:

- `account_sid`: Twilio Account SID
- `auth_token`: Twilio Authentication Token
- `to_number`: Required a phone number to send text messages to
- `from_number`: Required a phone number to send text messages from

## How to get your Twilio credentials

1. Sign up for a Twilio account or log into your existing account.
2. Navigate to the [Twilio Console Dashboard](https://www.twilio.com/console).
3. Here you will find your `ACCOUNT SID` and `AUTH TOKEN`.
4. To get a Twilio phone number, navigate to the "Phone Numbers" section and either use an existing number or buy a new one.
5. Store these as environment variables: `TWILIO_ACCOUNT_SID`, `TWILIO_AUTH_TOKEN`, and `TWILIO_PHONE_NUMBER` respectively.

## Trying out Whatsapp Conversations with Twilio

1. Navigate to this [guide](https://www.twilio.com/docs/conversations/use-twilio-sandbox-for-whatsapp) for complete documentation or follow the following steps.
2. Go to the [Twilio Console Dashboard](https://console.twilio.com/us1/develop/conversations/tryout/whatsapp)
3. Select "User-Initiated Conversation"
4. Either open whatsapp and send `join <Your Sandbox Keyword>` or `Scan the QR on the mobile phone`.

All things are in place now, in order to use the Whatsapp Using MindsDB.

## Implemented Features

- Send a Whatsapp Message to a given number with a specified body.
- Fetch the last `n` messages sent or received by the whatsapp number.

## Example Usage

```sql
-- Creating a Database
CREATE DATABASE whatsapp_test
WITH ENGINE = "whatsapp",
PARAMETERS = {
  "account_sid": "YOUR_ACCOUNT_SID",
  "auth_token": "YOUR_AUTH_TOKEN"
  };
```

You can now run queries as follows:

```sql
-- Get all messages
SELECT * FROM whatsapp_test.messages LIMIT 100;
```

```sql
-- Get message with sid
SELECT * FROM whatsapp_test.messages where sid="SM375f075778f91b56634ce5d92db249cd";
```

```sql
-- filter to and from
SELECT * FROM whatsapp_test.messages where from_number="whatsapp:+14155238886";
SELECT * FROM whatsapp_test.messages where to_number="whatsapp:+14155238886";
```

```sql
-- Send a whatsapp message
INSERT INTO whatsapp_test.messages (body, from_number, to_number)
VALUES('woww, such a cool integration', 'whatsapp:+14155238886', 'whatsapp:+14155238886');
```


# Twilio Handler

Twilio handler for MindsDB provides interfaces to connect to Twilio via APIs and send or retrieve SMS data into MindsDB.

---

## Table of Contents

- [Twilio Handler](#twilio-handler)
  - [Table of Contents](#table-of-contents)
  - [About Twilio](#about-twilio)
  - [Twilio Handler Implementation](#twilio-handler-implementation)
  - [Twilio Handler Initialization](#twilio-handler-initialization)
  - [How to get your Twilio credentials](#how-to-get-your-twilio-credentials)
  - [Implemented Features](#implemented-features)
  - [TODO](#todo)
  - [Example Usage](#example-usage)

---

## About Twilio

Twilio provides a cloud communication platform which allows software developers to programmatically make and receive phone calls, send and receive text messages, and perform other communication functions using its web service APIs.

## Twilio Handler Implementation

This handler was implemented using the [Twilio Python SDK](https://www.twilio.com/docs/libraries/python). The SDK provides a simple and efficient way to interact with the Twilio API.

## Twilio Handler Initialization

The Twilio handler is initialized with the following parameters:

- `account_sid`: a required Twilio Account SID
- `auth_token`: a required Twilio Authentication Token
- `phone_number`: a required Twilio phone number

## How to get your Twilio credentials

1. Sign up for a Twilio account or log into your existing account.
2. Navigate to the [Twilio Console Dashboard](https://www.twilio.com/console).
3. Here you will find your `ACCOUNT SID` and `AUTH TOKEN`.
4. To get a Twilio phone number, navigate to the "Phone Numbers" section and either use an existing number or buy a new one.
5. Store these as environment variables: `TWILIO_ACCOUNT_SID`, `TWILIO_AUTH_TOKEN`, and `TWILIO_PHONE_NUMBER` respectively.

## Implemented Features

- Send an SMS to a given number with a specified body.
- Fetch the last `n` messages sent or received by the Twilio phone number.

## TODO

- Implement support for making and receiving calls.
- Add more detailed logging and error handling.

## Example Usage

```sql
-- To send an SMS
CREATE DATABASE my_twilio
With 
    ENGINE = 'twilio',
    PARAMETERS = {
     "account_sid":"YOUR_ACCOUNT_SID",
     "auth_token":"YOUR_AUTH_TOKEN",
     "phone_number":"YOUR_TWILIO_PHONE_NUMBER"
    };
```

After setting up the Twilio Handler, you can use SQL commands to interact with Twilio:

```sql
-- Use a native command to send an SMS
NATIVE COMMAND ON my_twilio
WITH query = 'send_sms: {"to": "+1234567890", "body": "Hello from MindsDB!"}';
```

To fetch the last 10 messages:

```sql
-- Use a native command to fetch the last 10 messages
NATIVE COMMAND ON my_twilio
WITH query = 'fetch_messages: {"n": 10}';
```



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
     "auth_token":"YOUR_AUTH_TOKEN"
    };
```

You can now run queries as follows:

```sql
-- Get all messages 
SELECT * FROM my_twilio.messages LIMIT 100;

-- get all messages sent for all numbers in this account
SELECT * FROM 
    my_twilio.phone_numbers 
    LEFT JOIN my_twilio.messages 
    ON my_twilio.phone_numbers.phone_number = my_twilio.messages.from_number;

-- Get message with sid
SELECT sid, to_number, from_number FROM my_twilio.messages where sid="SMbefbd64e3caa7d4c147a0aab82d47";

-- filter to and from
SELECT * FROM my_twilio.messages where from_number="+15129222338";
SELECT * FROM my_twilio.messages where to_number="+15129222338";


-- send messages:
INSERT INTO my_twilio.messages (to_number, from_number, body)
values("+15129222338", "+16122530327", "wow! testing this");

select * from my_twilio(
fetch_messages(date_sent_after='2022-10-29 09:46:29.000000')
```

# Email Handler

Email handler for MindsDB provides interfaces to connect to email services via APIs and pull data into MindsDB. It is also possible to send emails from MindsDB using this handler.

---

## Table of Contents

- [Email Handler](#github-handler)
  - [Table of Contents](#table-of-contents)
  - [Email Handler Implementation](#sendinblue-handler-implementation)
  - [Email Handler Initialization](#sendinblue-handler-initialization)
  - [Implemented Features](#implemented-features)
  - [TODO](#todo)
  - [Example Usage](#example-usage)

---

## Email Handler Implementation

This handler was implemented using the standard Python libraries: email, imaplib and smtplib.

## Email Handler Initialization

The Email handler is initialized with the following required parameters:

- `email`: a required email address to use for authentication.
- `password`: a required password to use for authentication.

To use the handler on a Gmail account, the password must be an [app password](https://support.google.com/accounts/answer/185833?hl=en).

Additionally, the following optional parameters can be passed:

- `smtp_server`: SMTP server to use for sending emails. Defaults to `smtp.gmail.com`.
- `smtp_port`: SMTP port to use for sending emails. Defaults to `587`.
- `imap_server`: IMAP server to use for receiving emails. Defaults to `imap.gmail.com`.

At the moment, the handler has only been tested with Gmail accounts.

## Implemented Features

- [x] Emails Table for a email account
  - [x] Support SELECT
    - [x] Support LIMIT
    - [x] Support WHERE
    - [x] Support ORDER BY
    - [x] Support column selection
  - [x] Support INSERT: send emails
    - [x] Support to, subject and body columns

## TODO

- [ ] Test the handler for other email providers like Outlook, Yahoo, etc.

## Example Usage

The first step is to create a database with the new `email` engine by passing in the required `email` and `password` parameters:

~~~~sql
CREATE DATABASE email_datasource
WITH ENGINE = 'email',
PARAMETERS = {
  "email": "youremail@gmail.com",
  "password": "yourpassword"
};
~~~~

Use the established connection to query your emails:

~~~~sql
SELECT * FROM email_datasource.emails
~~~~

Run more advanced queries:

~~~~sql
SELECT  to, subject, body
FROM email_datasource.emails
WHERE subject = 'MindsDB'
ORDER BY id
LIMIT 5
~~~~

Send emails:

~~~~sql
INSERT INTO email_datasource.emails(to, subject, body)
VALUES ("toemail@gmail.com", "MindsDB", "Hello from MindsDB!")
~~~~
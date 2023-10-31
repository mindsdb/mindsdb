# Microsoft Teams Handler

Microsoft Teams handler for MindsDB provides interfaces to connect to Microsoft Teams via a webhook and send messages through MindsDB.

---

## Table of Contents

- [Microsoft Teams Handler](#microsoft-teams-handler)
  - [Table of Contents](#table-of-contents)
  - [About Microsoft Teams](#about-microsoft-teams)
  - [Microsoft Teams Handler Implementation](#microsoft-teams-handler-implementation)
  - [Microsoft Teams Handler Initialization](#microsoft-teams-handler-initialization)
  - [Implemented Features](#implemented-features)
  - [TODO](#todo)
  - [Example Usage](#example-usage)

---

## About Microsoft Teams

Microsoft Teams is the ultimate messaging app for your organization—a workspace for real-time collaboration and communication, meetings, file and app sharing, and even the occasional emoji! All in one place, all in the open, all accessible to everyone.
<br>
https://support.microsoft.com/en-us/topic/what-is-microsoft-teams-3de4d369-0167-8def-b93b-0eb5286d7a29

## Microsoft Teams Handler Implementation

This handler was implemented using [pymsteams](https://pypi.org/project/pymsteams/), the Python Wrapper Library to send requests to Microsoft Teams Webhooks.

## Microsoft Teams Handler Initialization

The Microsoft Teams handler is initialized with the following parameters:

- `webhook_url`: a required webhook url for the Microsoft Teams channel to send messages to

Read about creating an Incoming Webhook in Microsoft Teams [here](https://learn.microsoft.com/en-us/microsoftteams/platform/webhooks-and-connectors/how-to/add-incoming-webhook?tabs=dotnet).

## Implemented Features

- [x] Send messages to a Microsoft Teams channel via a webhook

## TODO

- [ ] Support other options for sending messages such as sections, images, actions, etc.
- [ ] Support INSERT, UPDATE and DELETE for messages table

## Example Usage

The first step is to create a database with the new `teams` engine by passing in the required `webhook_url` parameter:

~~~~sql
CREATE DATABASE teams_datasource
WITH ENGINE = 'teams',
PARAMETERS = {
  "webhook_url": "https://..."
};
~~~~

Use the established connection to send messages to your Teams channel:

~~~~sql
INSERT INTO teams_datasource.messages (title, text)
VALUES ('Hello', 'World');
~~~~

Multiple messages can also be sent in a single query:

~~~~sql
INSERT INTO teams_datasource.messages (title, text)
VALUES 
('Hello', 'World'),
('Hello', 'MindsDB');
~~~~

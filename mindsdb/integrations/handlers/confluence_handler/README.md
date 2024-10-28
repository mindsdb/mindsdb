# Confluence Handler

Confluence handler for MindsDB provides interfaces to connect with Confluence via APIs and pull confluence data into MindsDB.

## Confluence

Confluence is a collaborative documentation tool,it can be used to host Wiki pages. In this handler,python client of api is used and more information about this python client can be found [here](https://pypi.org/project/atlassian-python-api/)

## Confluence Handler Initialization

The Confluence handler is initialized with the following parameters:

- `url`: Confluence hosted url instance
- `confluence_api_token`: Confluence API key to use for authentication

Please follow this [link](https://docs.searchunify.com/Content/Content-Sources/Atlassian-Jira-Confluence-Authentication-Create-API-Token.htm) to generate the token for accessing confluence API

## Implemented Features

- [x] Confluence spaces table for a given confluence hosted url instance
  - [x] Support LIMIT
  - [x] Support WHERE
  - [x] Support ORDER BY
  - [x] Support column selection

## Example Usage

The first step is to create a database with the new `confluence` engine.

~~~~sql
CREATE DATABASE mindsdb_confluence
WITH
    ENGINE = 'confluence',
    PARAMETERS = {
    "url": "https://marios.atlassian.net/",
    "username": "your_username",
    "password":"access_token"
    };
~~~~

Use the established connection to query your database:

~~~~sql
SELECT * FROM mindsdb_confluence.pages
WHERE space='space';
~~~~

Advanced queries for the confluence handler

~~~~sql
SELECT id,title,body
FROM mindsdb_confluence.pages
WHERE space='space'
ORDER BY id ASC, title DESC
LIMIT 10
~~~~

## CRUD Operations

The engine supports CRUD operations using SQL queries on the `pages` Content Type.
Here is the format for the queries:

~~~~sql
SELECT * FROM mindsdb_confluence.pages
WHERE space='space';
~~~~

You need to specify the name of the space to view all the pages in that space.

To select a single page, you can use the `id` column:
~~~~sql
SELECT * FROM mindsdb_confluence.pages
WHERE space='space' AND id=123456;
~~~~

~~~~sql
INSERT INTO confluence_data.pages ('space', 'title', 'body')
VALUES
('DEMO', 'test title # 1', 'test body # 1'),
('DEMO', 'test title # 2', 'test body # 2'),
('DEMO', 'test title # 3', 'test body # 3')
~~~~

You need to specify the name of the space, title and body of the page to create a new page in that space.

~~~~sql
UPDATE confluence_data.pages
SET title='New Title', body='This is the new body'
WHERE id=123456;
~~~~

You need to specify the id of the page to update the title and body of the page.

~~~~sql
DELETE FROM confluence_data.pages
WHERE id=123456;
~~~~

You need to specify the id of the page to delete the page.

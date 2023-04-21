# Confluence Handler

Confluence handler for MindsDB provides interfaces to connect with Confluence via APIs and pull confluence data into MindsDB.

## Confluence

Confluence is a collaborative documentation tool,it can be used to host Wiki pages. In this handler,python client of api is used and more information about this python client can be found (here)[https://pypi.org/project/atlassian-python-api/]

## Confluence Handler Initialization

The Confluence handler is initialized with the following parameters:

- `url`: Confluence hosted url instance
- `confluence_api_token`: Confluence API key to use for authentication

Please follow this (link)[https://docs.searchunify.com/Content/Content-Sources/Atlassian-Jira-Confluence-Authentication-Create-API-Token.htm] to generate the token for accessing confluence API

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
WITH ENGINE = 'confluence',
PARAMETERS = {
  "url": "https://wiki.onap.org/",
  "confluence_api_token": "MDk5NzgyNzY1Mzc3OlP5jEKnCL/z1+jIyEfIVIwERbJF"  
};
~~~~

Use the established connection to query your database:

~~~~sql
SELECT * FROM mindsdb_confluence.pages
~~~~

Advanced queries for the confluence handler

~~~~sql
SELECT id,key,name,type
FROM mindsdb_confluence.pages
WHERE type="personal"
ORDER BY id ASC, name DESC
LIMIT 10
~~~~

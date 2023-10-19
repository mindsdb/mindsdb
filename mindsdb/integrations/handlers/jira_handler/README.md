# Jira Handler

This is the implementation of the Jira handler for MindsDB.

## Jira
In short, Jira is a tool to track the progress of software defects,story and releases.
In this handler. python client of api is used and more information about this python client can be found (here)[https://pypi.org/project/atlassian-python-api/]


## Implementation
This handler was implemented as per the MindsDB API Handler documentation.


The required arguments to establish a connection are,
* `jira_url`: Jira  hosted url instance
* `jira_api_token`: API key for accessing the Jira url instance
* `project`: Jira project name 


## Usage
In order to make use of this handler and connect to an Jira in MindsDB, the following syntax can be used,
~~~~sql
CREATE DATABASE jira_source
WITH
engine='Jira',
parameters={
    "jira_url": "https://jira.linuxfoundation.org",
     "jira_api_token": "Bearer <your-jira-api-token>",
     "project": "RELENG"   
};
~~~~

## Implemented Features

- [x] Jira project table for a given Jira hosted url instance
  - [x] Support LIMIT
  - [x] Support ORDER BY
  - [x] Support column selection

Now, you can use this established connection to query your table as follows,
~~~~sql
SELECT * FROM jira_source.project
~~~~

Advanced queries for the jira handler

~~~~sql
SELECT key,summary,status
FROM jira_source.project
ORDER BY key ASC
LIMIT 10
~~~~ 

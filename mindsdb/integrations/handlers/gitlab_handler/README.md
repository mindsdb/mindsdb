# GitLab Handler

This is the implementation of the GitLab handler for MindsDB. This interface support to connect to GitLab API and pull data into MindsDB

## GitLab Handler Implementation

This handler was implemented using the [python-gitlab](https://github.com/python-gitlab/python-gitlab) library.
python-gitlab is a Python library that wraps GitLab API.

## GitLab Handler Initialization

The GitLab handler is initialized with the following parameters:

- `repository`: a required name of a GitLab repository to connect to
- `api_key`: an optional GitLab API key to use for authentication

## Implemented Features

- GitLab Issues table for a given Repository (Support LIMIT, WHERE, ORDER BY, SELECT - column)

## Usage
In order to make use of this handler and connect to a gitlab api in MindsDB, the following syntax can be used,

~~~~sql
CREATE DATABASE mindsdb_gitlab
WITH ENGINE = 'gitlab',
PARAMETERS = {
  "repository": "gitlab-org/gitlab",
  "api_key": "api_key",    -- optional GitLab API key
};
~~~~

Now, you can use this established connection to query your table as follows,
~~~~sql
SELECT * FROM mindsdb_gitlab.issues
~~~~

~~~~sql
SELECT number, state, creator, assignee, title, labels
  FROM mindsdb_gitlab.issues
  WHERE state="open"
  ORDER BY created ASC, creator DESC
  LIMIT 10
~~~~
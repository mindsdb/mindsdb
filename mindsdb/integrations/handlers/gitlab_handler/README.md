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

 [x] GitLab Issues Table for a given Repository
  - [x] Support SELECT
    - [x] Support LIMIT
    - [x] Support WHERE
    - [x] Support ORDER BY
    - [x] Support column selection
- [x] GitLab Merge Requests Table for a given Repository
  - [x] Support SELECT
    - [x] Support LIMIT
    - [x] Support WHERE
    - [x] Support ORDER BY
    - [x] Support column selection


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
SELECT * FROM mindsdb_gitlab.issues;
~~~~

~~~~sql
SELECT number, state, creator, assignee, title, created, labels 
  FROM mindsdb_gitlab.issues
  WHERE state="opened"
  ORDER BY created ASC, creator DESC
  LIMIT 10;
~~~~

~~~~sql
SELECT number, state, creator, reviewers, title, created, has_conflicts
  FROM mindsdb_gitlab.merge_requests
  WHERE state="merged"
  ORDER BY created ASC, creator DESC
  LIMIT 10;
~~~~

## What is next??

Add support for:

- GitLab Branches, Releases, Branches tables for a given Repository 

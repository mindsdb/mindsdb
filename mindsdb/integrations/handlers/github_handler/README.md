# GitHub Handler

GitHub handler for MindsDB provides interfaces to connect to GitHub via APIs and pull repository data into MindsDB.

---

## Table of Contents

- [GitHub Handler](#github-handler)
  - [Table of Contents](#table-of-contents)
  - [About GitHub](#about-github)
  - [GitHub Handler Implementation](#github-handler-implementation)
  - [GitHub Handler Initialization](#github-handler-initialization)
  - [Implemented Features](#implemented-features)
  - [Example Usage](#example-usage)

---

## About GitHub

GitHub is a web-based hosting service for version control using Git. It is mostly used for computer code.
It offers all the distributed version control and source code management (SCM) functionality
of Git as well as adding its own features. It provides access control and several collaboration
features such as bug tracking, feature requests, task management, and wikis for every project.

## GitHub Handler Implementation

This handler was implemented using the [pygithub](https://github.com/PyGithub/PyGithub) library.
PyGithub is a Python library that wraps GitHub API v3.

## GitHub Handler Initialization

The GitHub handler is initialized with the following parameters:

- `repository`: GitHub repository name (`owner/repo`). Optional — can be specified per query via `WHERE repository = 'owner/repo'`
- `api_key`: an optional GitHub personal access token for authentication
- `github_url`: an optional GitHub URL to connect to a GitHub Enterprise instance

**GitHub App OAuth** (alternative to `api_key` — for platform integrations):

- `client_id`: the GitHub App's OAuth client ID
- `client_secret`: the GitHub App's OAuth client secret
- `access_token`: OAuth access token obtained from the authorization code exchange
- `refresh_token`: OAuth refresh token for automatic token renewal

Your platform handles the OAuth redirect and code exchange, then passes the resulting tokens to MindsDB. The handler automatically refreshes expired tokens using the rotating refresh token pattern.

Read about [creating a GitHub personal access token](https://docs.github.com/en/github/authenticating-to-github/creating-a-personal-access-token).
Read about [creating a GitHub App](https://docs.github.com/en/apps/creating-github-apps/about-creating-github-apps/about-creating-github-apps).

## Implemented Features

- [x] GitHub Issues Table for a given Repository
  - [x] Support SELECT
    - [x] Support LIMIT
    - [x] Support WHERE
    - [x] Support ORDER BY
    - [x] Support column selection
  - [x] Support INSERT
    - [x] Support title, body, assignee, milestone, and labels columns
- [x] GitHub Pull Requests Table for a given Repository
- [x] GitHub Commits Table for a given Repository
- [x] GitHub Releases Table for a given Repository
- [x] GitHub Branches Table for a given Repositorytory
- [x] GitHub Projects Table for a given Repository
  - [x] Support SELECT
    - [x] Support LIMIT
    - [x] Support WHERE
    - [x] Support ORDER BY
    - [x] Support column selection
- [x] GitHub Check Runs Table (requires `WHERE sha='...'` or `branch='...'`)
  - [x] Support SELECT with LIMIT, WHERE (sha, branch, name)
- [x] GitHub Commit Statuses Table (requires `WHERE sha='...'` or `branch='...'`)
  - [x] Support SELECT with LIMIT, WHERE (sha, branch)
- [x] GitHub Pages Builds Table for a given Repository
  - [x] Support SELECT with LIMIT

## Example Usage

The first step is to create a database with the new `github` engine. The `api_key` parameter is optional,
however, GitHub aggressively rate limits unauthenticated users. Read about [creating a GitHub personal access token](https://docs.github.com/en/github/authenticating-to-github/creating-a-personal-access-token).

~~~~sql
CREATE DATABASE mindsdb_github
WITH ENGINE = 'github',
PARAMETERS = {
  "repository": "mindsdb/mindsdb",
  "api_key": "your_api_key"    -- optional GitHub API key
};
~~~~

Alternatively, connect using a **GitHub App OAuth** (for platform integrations with automatic token refresh):

~~~~sql
CREATE DATABASE mindsdb_github
WITH ENGINE = 'github',
PARAMETERS = {
  "repository": "org/repo",
  "client_id": "Iv1.abc123",
  "client_secret": "your_client_secret",
  "access_token": "ghu_xxx",
  "refresh_token": "ghr_xxx"
};
~~~~

You can also create a connection **without a default repository** and specify it per query:

~~~~sql
CREATE DATABASE github_conn
WITH ENGINE = 'github',
PARAMETERS = {
  "client_id": "Iv1.abc123",
  "client_secret": "your_client_secret",
  "access_token": "ghu_xxx",
  "refresh_token": "ghr_xxx"
};
~~~~

~~~~sql
SELECT * FROM github_conn.issues WHERE repository = 'org/repo' LIMIT 5;
SELECT * FROM github_conn.issues WHERE repository = 'org/other-repo' LIMIT 5;
~~~~

Use the established connection to query your database:

~~~~sql
SELECT * FROM mindsdb_github.issues
~~~~

~~~~sql
SELECT * FROM mindsdb_github.branches
~~~~

~~~~sql
SELECT * FROM mindsdb_github.contributors
~~~~

~~~~sql
SELECT * FROM mindsdb_github.projects
~~~~

Run more advanced queries:

~~~~sql
SELECT number, state, creator, assignee, title, labels
  FROM mindsdb_github.issues
  WHERE state="all"
  ORDER BY created ASC, creator DESC
  LIMIT 10
~~~~

~~~~sql
SELECT number, state, title, creator, head, commits
  FROM mindsdb_github.pull_requests
  WHERE state="all"
  ORDER BY long_running DESC, commits DESC
  LIMIT 10
~~~~

Query check runs for a specific branch or commit:

~~~~sql
SELECT name, status, conclusion, started_at, completed_at, app_name
  FROM mindsdb_github.check_runs
  WHERE branch = 'main'
  LIMIT 20
~~~~

~~~~sql
SELECT name, status, conclusion
  FROM mindsdb_github.check_runs
  WHERE sha = 'abc123def456'
~~~~

Query commit statuses (CI/CD status checks):

~~~~sql
SELECT state, context, description, target_url, created_at
  FROM mindsdb_github.commit_statuses
  WHERE branch = 'main'
~~~~

Query GitHub Pages build history:

~~~~sql
SELECT status, pusher, commit, duration, created_at
  FROM mindsdb_github.pages_builds
  LIMIT 10
~~~~



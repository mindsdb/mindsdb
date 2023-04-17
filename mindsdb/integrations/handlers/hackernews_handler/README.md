# HackerNews Handler

HackerNews handler for MindsDB provides interfaces to connect to HackerNews via APIs and pull data into MindsDB.

---

## Table of Contents

- [HackerNews Handler](#reddit-handler)
  - [Table of Contents](#table-of-contents)
  - [About HackerNews](#about-reddit)
  - [HackerNews Handler Implementation](#reddit-handler-implementation)
  - [HackerNews Handler Initialization](#reddit-handler-initialization)
  - [Implemented Features](#implemented-features)
  - [TODO](#todo)
  - [Example Usage](#example-usage)

---

## About HackerNews

HackerNews is a network of communities based on people's interests. It provides a platform for users to submit links, create content, and have discussions about various topics.

## HackerNews Handler Implementation

This handler was implemented using the [PRAW (Python HackerNews API Wrapper)](https://praw.readthedocs.io/en/latest/) library. PRAW is a Python package that provides a simple and easy-to-use interface to access the HackerNews API.

## HackerNews Handler Initialization

The HackerNews handler is initialized with the following parameters:

- `client_id`: a required HackerNews API client ID
- `client_secret`: a required HackerNews API client secret
- `user_agent`: a required user agent string to identify your application

Read about creating a HackerNews API application [here](https://www.reddit.com/prefs/apps).

## Implemented Features

- Fetch submissions from a subreddit based on sorting type and limit.
- (Add other implemented features here)

## TODO

- (List any pending features or improvements here)

## Example Usage
```
CREATE DATABASE my_reddit
With 
    ENGINE = 'reddit',
    PARAMETERS = {
     "client_id":"YOUR_CLIENT_ID",
     "client_secret":"YOUR_CLIENT_SECRET",
     "user_agent":"YOUR_USER_AGENT"
    };
```

After setting up the HackerNews Handler, you can use SQL queries to fetch data from HackerNews:

```sql
SELECT *
FROM my_red.submission
WHERE subreddit = 'MachineLearning' AND sort_type = 'top' AND limit = 5;
```

Each Post in a subreddit has a unique ID. You can use this ID to fetch comments for a particular post.

```
SELECT *
FROM my_red.comment
WHERE submission_id = '12gls93'
```
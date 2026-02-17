# Reddit Handler

Reddit handler for MindsDB provides interfaces to connect to Reddit via APIs and pull data into MindsDB.

---

## Table of Contents

- [Reddit Handler](#reddit-handler)
  - [Table of Contents](#table-of-contents)
  - [About Reddit](#about-reddit)
  - [Reddit Handler Implementation](#reddit-handler-implementation)
  - [Reddit Handler Initialization](#reddit-handler-initialization)
  - [Implemented Features](#implemented-features)
  - [TODO](#todo)
  - [Example Usage](#example-usage)

---

## About Reddit

Reddit is a network of communities based on people's interests. It provides a platform for users to submit links, create content, and have discussions about various topics.

## Reddit Handler Implementation

This handler was implemented using the [PRAW (Python Reddit API Wrapper)](https://praw.readthedocs.io/en/latest/) library. PRAW is a Python package that provides a simple and easy-to-use interface to access the Reddit API.

## Reddit Handler Initialization

The Reddit handler is initialized with the following parameters:

- `client_id`: a required Reddit API client ID
- `client_secret`: a required Reddit API client secret
- `user_agent`: a required user agent string to identify your application

## How to get your Reddit credentials.

1. Visit Reddit App Preferences (https://www.reddit.com/prefs/apps) or [https://old.reddit.com/prefs/apps/](https://old.reddit.com/prefs/apps/)
2. Scroll to the bottom and click "create another app..."
3. Fill out the name, description, and redirect url for your app, then click "create app"
4. Now you should be able to see the personal use script, secret, and name of your app. Store those as environment variables CLIENT_ID, CLIENT_SECRET, and USER_AGENT respectively.

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

After setting up the Reddit Handler, you can use SQL queries to fetch data from Reddit:

```sql
SELECT *
FROM my_reddit.submission
WHERE subreddit = 'MachineLearning' AND sort_type = 'top' AND items = 5;
```

`items`: Number of items to fetch from the subreddit.

`sort_type`: Sorting type for the subreddit. Can be one of `hot`, `new`, `top`, `controversial`, `gilded`, `wiki`, `mod`, `rising`.

Each Post in a subreddit has a unique ID. You can use this ID to fetch comments for a particular post.

```
SELECT *
FROM my_reddit.comment
WHERE submission_id = '12gls93'
```
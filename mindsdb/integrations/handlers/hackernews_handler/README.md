# HackerNews Handler

HackerNews handler for MindsDB provides interfaces to connect to HackerNews via APIs and pull data into MindsDB.

---

## Table of Contents

- [About HackerNews](#about-hackernews)
  - [HackerNews Handler Implementation](#hackernews-handler-implementation)
  - [Implemented Features](#implemented-features)
  - [TODO](#todo)
  - [Example Usage](#example-usage)
---
## About HackerNews

HackerNews is a social news website that provides a platform for users to submit links, create content, and have discussions about various topics. It was created by the startup incubator Y Combinator.

## HackerNews Handler Implementation

This handler was implemented using the official HackerNews API. It provides a simple and easy-to-use interface to access the HackerNews API.


## Implemented Features

- Fetch submissions from a subreddit based on sorting type and limit.
- (Add other implemented features here)

## TODO

- (List any pending features or improvements here)

## Example Usage
```
CREATE DATABASE my_hackernews
WITH 
ENGINE = 'hackernews'
```

After setting up the HackerNews Handler, you can use SQL queries to fetch data from HackerNews:

```sql
SELECT *
FROM my_hackernews.stories
LIMIT 2;
```

OR

```sql
SELECT *
FROM my_hackernews.hnstories
LIMIT 5;
```

OR

```SQL
SELECT *
FROM my_hackernews.jobstories
LIMIT 7;
```

OR

```sql
SELECT *
FROM my_hackernews.showstories
LIMIT 5;
```

Each Post has a unique ID. You can use this ID to fetch comments for a particular post.

```sql
SELECT *
FROM my_hackernews.comments
WHERE item_id=35662571
LIMIT 1;
```
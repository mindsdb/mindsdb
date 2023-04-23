# Quickbooks Handler

Quickbooks handler for MindsDB provides interfaces to connect to Quickbooks via APIs and pull data into MindsDB.

---

## Table of Contents

- [About Quickbooks](#about-Quickbooks)
  - [Quickbooks Handler Implementation](#Quickbooks-handler-implementation)
  - [Implemented Features](#implemented-features)
  - [TODO](#todo)
  - [Example Usage](#example-usage)
---
## About Quickbooks

Quickbooks is a social news website that provides a platform for users to submit links, create content, and have discussions about various topics. It was created by the startup incubator Y Combinator.

## Quickbooks Handler Implementation

This handler was implemented using the official Quickbooks API. It provides a simple and easy-to-use interface to access the Quickbooks API.


## Implemented Features

- Fetch submissions from a subreddit based on sorting type and limit.
- (Add other implemented features here)

## TODO

- (List any pending features or improvements here)

## Example Usage
```
CREATE DATABASE my_Quickbooks;
With 
    ENGINE = 'Quickbooks',
```

After setting up the Quickbooks Handler, you can use SQL queries to fetch data from Quickbooks:

```sql
SELECT *
FROM my_Quickbooks.stories
LIMIT 2;
```

Each Post has a unique ID. You can use this ID to fetch comments for a particular post.

```
SELECT *
FROM mysql_datasource.comments
WHERE item_id=35662571
LIMIT 1;
```
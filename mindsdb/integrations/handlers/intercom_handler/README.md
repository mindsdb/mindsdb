## Intercom API Handler Initialization

The Intercom API handler can be initialized with the following parameters:

- `access_token` - your Intercom access token

## Implemented Features

- `Articles` - retrieve articles from Intercom
  - `SELECT` - retrieve data from Intercom, such as user information, conversations, etc.
  - `WHERE` - filter data based on specific criteria.
  - `INSERT` - create new records, like new users or conversations.
  - `UPDATE` - update existing records, such as updating user information or conversation details.

## Usage

To create a database with the Intercom engine, you can use SQL-like syntax:

```sql
CREATE DATABASE myintercom
WITH
  ENGINE = 'intercom',
  PARAMETERS = {
    "access_token" : "<your-intercom-access-token>"
  };
```

### SELECT

You can retrieve data from Intercom using a `SELECT` statement:

```sql
SELECT *
FROM myintercom.articles;
```

### WHERE

You can filter data based on specific criteria using a `WHERE` clause:

```sql
SELECT *
FROM myintercom.articles
WHERE id = <article-id>;
```

### INSERT

You can create new article in Intercom using the `INSERT` statement:

```sql
INSERT INTO myintercom.articles (title, description, body, author_id, state, parent_id, parent_type)
VALUES (
  'Thanks for everything',
  'Description of the Article',
  'Body of the Article',
  6840572,
  'published',
  6801839,
  'collection'
);
```

### UPDATE

You can update existing records in Intercom using the `UPDATE` statement:

```sql
UPDATE myintercom.articles
SET title = 'Christmas is here!',
    body = '<p>New gifts in store for the jolly season</p>'
WHERE id = <article-id>;
```

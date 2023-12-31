## Intercom API Handler Initialization

The Intercom API handler can be initialized with the following parameters:

- `access_token` - your Intercom access token

## Implemented Features

- [x] Articles
  - [x] Support SELECT
    - [x] Support LIMIT
    - [x] Support WHERE
    - [x] Support column selection
  - [x] Support Insert
  - [x] Support UPDATE

## TODO:

- [ ] Implement `ORDER BY`, `DELETE` for Articles
- [ ] Intercom Admins table (Follow: [Admins](https://developers.intercom.com/intercom-api-reference/reference#admins))
- [ ] Intercom Companies table (Follow: [Companies](https://developers.intercom.com/docs/references/rest-api/api.intercom.io/Companies/))
- [ ] Intercom Contacts table (Follow: [Contacts](https://developers.intercom.com/docs/references/rest-api/api.intercom.io/Contacts/))
- [ ] Intercom Conversations table (Follow: [Conversations](https://developers.intercom.com/docs/references/rest-api/api.intercom.io/Conversations/))
- [ ] Intercom Data Attributes table (Follow: [Data Attributes](https://developers.intercom.com/docs/references/rest-api/api.intercom.io/Data-Attributes/))
- [ ] Intercom Data Events table (Follow: [Events](https://developers.intercom.com/docs/references/rest-api/api.intercom.io/Data-Events/))
- [ ] Intercom Data Export table (Follow : [Data Export](https://developers.intercom.com/docs/references/rest-api/api.intercom.io/Data-Export/))
- [ ] Intercom Help Center table (Follow: [Help Center](https://developers.intercom.com/docs/references/rest-api/api.intercom.io/Help-Center/))
- [ ] Intercom Messages table (Follow: [Messages](https://developers.intercom.com/docs/references/rest-api/api.intercom.io/Messages/))
- [ ] Intercom News table (Follow: [News](https://developers.intercom.com/docs/references/rest-api/api.intercom.io/News/))
- [ ] Intercom Notes table (Follow: [Notes](https://developers.intercom.com/docs/references/rest-api/api.intercom.io/Notes/))
- [ ] Intercom Subscriptions table (Follow: [Subscriptions](https://developers.intercom.com/docs/references/rest-api/api.intercom.io/Subscription-Types/))
- [ ] Intercom Tags table (Follow: [Tags](https://developers.intercom.com/docs/references/rest-api/api.intercom.io/Tags/))
- [ ] Intercom Tickets table (Follow: [Tickets](https://developers.intercom.com/docs/references/rest-api/api.intercom.io/Tickets/))
- [ ] Intercom Teams table (Follow: [Teams](https://developers.intercom.com/docs/references/rest-api/api.intercom.io/Teams/))
- [ ] Intercom Visitors table (Follow: [Visitors](https://developers.intercom.com/docs/references/rest-api/api.intercom.io/Visitors/))

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

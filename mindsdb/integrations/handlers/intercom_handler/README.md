## Intercom API Handler Initialization

The Intercom API handler can be initialized with the following parameters:

- `access_token` - your Intercom access token

## Implemented Features

- [x] Articles
  - [x] Support SELECT
  - [x] Support Insert
  - [x] Support UPDATE
- [x] Conversations
  - [x] Support SELECT
  - [x] Support Insert
  - [x] Support UPDATE
## TODO:

- [ ] Intercom Admins table (Follow: [Admins](https://developers.intercom.com/intercom-api-reference/reference#admins))
- [ ] Intercom Companies table (Follow: [Companies](https://developers.intercom.com/docs/references/rest-api/api.intercom.io/Companies/))
- [ ] Intercom Contacts table (Follow: [Contacts](https://developers.intercom.com/docs/references/rest-api/api.intercom.io/Contacts/))
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

## Connection

To create a database with the Intercom engine, you can use SQL-like syntax:

```sql
CREATE DATABASE myintercom
WITH
  ENGINE = 'intercom',
  PARAMETERS = {
    "access_token" : "<your-intercom-access-token>"
  };
```
## Usage (Articles Table)
### SELECT

```sql
SELECT *
FROM myintercom.articles;
```

### WHERE

```sql
SELECT *
FROM myintercom.articles
WHERE id = <article-id>;
```

### INSERT

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

```sql
UPDATE myintercom.articles
SET title = 'Christmas is here!',
    body = '<p>New gifts in store for the jolly season</p>'
WHERE id = <article-id>;
```

## Usage (Conversations Table)
### SELECT

```sql
SELECT *
FROM myintercom.conversations;
```

### WHERE

```sql
SELECT *
FROM myintercom.conversations
WHERE id = <conversation-id>;
```

### INSERT

```sql
INSERT INTO myintercom.conversations (type, id, body)
VALUES ('user', '6547130cf077c0c9a9003ef1', 'Hello there');
```

### UPDATE

```sql
UPDATE myintercom.conversations
SET `read` = true,
    issue_type = 'Billing',
    priority = 'High'
WHERE id = 5;
```

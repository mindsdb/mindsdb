# Notion Handler

This is the implementation of the Notion handler for MindsDB.

## Notion
[Notion](https://www.notion.so/help/guides/what-is-notion) is a note-taking and productivity freemium cloud platform.
In short, notion has all-in-one workspace tool that integrates kanban boards, tasks, wikis, and database.

## Implementation
This handler was implemented using notion-client library, a simple and easy to use client library for the official [Notion API](https://developers.notion.com/).


The required arguments to establish a connection are,
* `api_token`: API key for acessing the Notion instance.

NOTE: To access any database or page, the `api_token` must be added as it's connection. To add that as a connection, navigate to the page/database settings and add connection and enter the name of the api token integration.

## Usage
In order to make use of this handler and connect to an Notion in MindsDB, the following syntax can be used,

```sql
CREATE DATABASE notion_source
WITH
engine='notion',
parameters={
    "api_token": "<notion-api-token>",
};
```

## Implemented Features

Now, you can use this established connection to query your table as follows:

### Database

[Databases](https://developers.notion.com/reference/database) are like a collection of pages. It also has properties allowing to store and organize data in a structured way.

```sql
SELECT * FROM notion_test.database where database_id='<your-db-id>';
```

NOTE: In order to fetch database from notion, mindsdb will require the database_id.

> To get a database_id, just navigate to the database web page and copy the URL, the id then is the portion between the `/` and `?`
>
> Example:
>
> link: `https://www.notion.so/b144844d418f404a8f0f1da2a63cdf7c?v=25544d8fe08f4b06a4cecceabc49ff6a&pvs=4`
>
> Then the database_id is `b144844d418f404a8f0f1da2a63cdf7c`

### Pages

A [Page](https://developers.notion.com/reference/page) is a collection of blocks. It also has properties(if in a database) allowing to store and organize data in a structured way.

```sql
SELECT * FROM notion_test.pages where page_id='<your-page-id>';
```

NOTE: In order to fetch a page from notion, mindsdb will require the page_id.

> To get the page_id, copy the URL and select the id comming after the `title` of the page and the `-` .
>
> Example:
>
> link: `https://www.notion.so/From-Mindsdb-10eeb618d598468ba81c8d83da9613ff`
>
> Then the page_id is `10eeb618d598468ba81c8d83da9613ff`

### Blocks

A [Block](https://developers.notion.com/reference/block) is a piece of content in a page. It could be anything from a paragraph, link, image, table, etc.

```sql
SELECT * FROM notion_test.blocks where block_id='<your-block-id>';
```

NOTE: In order to fetch a block from notion, mindsdb will require the block_id.

> To get the block id, right clicking the block will open a options tray and click `Copy link to block` this will provide the link to the block for that page. Then the block id is the fragment in that link i.e. after the part after the `#` in the link
>
> Example
>
> link: `https://www.notion.so/From-Mindsdb-10eeb618d598468ba81c8d83da9613ff?pvs=4#3666b704803140e2b60cadfcc59cf66a`
>
> Then the block_id is `3366b704803140e2b60cadfcc59cf66a`

### Comments for a Block

```sql
SELECT * FROM notion_test.comments where block_id='<your-block-id>';
```

### Create a page in a database

```sql
INSERT INTO notion_test.pages (title, text, database_id)
VALUES ('Hello World', 'MindsDB is awesome. You should check it out', '<your-database-id>');
```

### Run native query as a python function

```sql
SELECT * FROM notion_test
(
    databases.query(database_id=b144844d418f404a8f0f1da2a63cdf7c)
);
```

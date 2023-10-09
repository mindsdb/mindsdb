# Notion Handler

This is the implementation of the Notion handler for MindsDB.

## Notion
Notion is a note-taking and productivity freemium cloud platform.
In short, notion has all-in-one workspace tool that integrates kanban boards, tasks, wikis, and database.
In this handler. python client of api is used and more information about this python client can be found (here)[https://pypi.org/project/notion-client/]


## Implementation
This handler was implemented as per the MindsDB API Handler documentation.


The required arguments to establish a connection are,
* `notion_api_token`: API key for acessing the Notion instance.


## Usage
In order to make use of this handler and connect to an Notion in MindsDB, the following syntax can be used,
~~~~sql
CREATE DATABASE notion_source
WITH
engine='notion',
parameters={
    "notion_api_token": "<notion-api-token>",
};
~~~~

## Implemented Features

Now, you can use this established connection to query your table as follows,

### Database

~~~~sql
SELECT * FROM notion_test.database where database_id='<your-db-id>';
~~~~

### Pages

```sql
SELECT * FROM notion_test.pages where page_id='<your-page-id>';
```

### Blocks

```sql
SELECT * FROM notion_test.blocks where block_id='<your-block-id>';
```

### Comments for a Block

```sql
SELECT * FROM notion_test.comments where block_id='<your-block-id>';
```

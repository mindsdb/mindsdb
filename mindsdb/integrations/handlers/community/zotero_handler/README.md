# Build your own Zotero AI agent

This is the implementation of the Zotero handler for MindsDB.

## Zotero
[Zotero](https://www.zotero.org/) is a free tool for organizing and managing research materials. It lets you store articles, books, and other resources in one place, annotate them, take notes, and create collections for organization. You can also share your collections with others.

## Implementation
This handler uses [pyzotero](https://pyzotero.readthedocs.io/en/latest/) , an API wrapper for Zotero as a simple library to facilitate the integration.

The required arguments to establish a connection are,
- `library_id` :   
To find your library ID, as noted on [pyzotero](https://pyzotero.readthedocs.io/en/latest/) :  
  - Your personal library ID is available [here](https://www.zotero.org/settings/keys), in the section "Your userID for use in API calls" - you must be logged in for the link to work.   
  - For group libraries, the ID can be found by opening the group’s page: https://www.zotero.org/groups/groupname, and hovering over the group settings link. The ID is the integer after /groups/
- `library_type` : "user" for personal account or "group" for a group library
- `api_key` :    
To find your api key, as noted on [pyzotero](https://pyzotero.readthedocs.io/en/latest/) :  follow [this link](https://www.zotero.org/settings/keys/new)

## Usage
In order to make use of this handler and connect to Zotero in MindsDB, the following syntax can be used,

```sql
CREATE DATABASE mindsdb_zotero
WITH ENGINE = 'zotero',
PARAMETERS = {
  "library_id": "<your-library-id>",
  "library_type": "user",
  "api_key": "<your-api-key>" }
```
 
## Implemented Features

Now, you can use this established connection to query your table as follows:

Important note: Most things (articles, books, annotations, notes etc.) are refered to as **items** in the pyzotero API. So each item has an item_type (such as "annotation") and an item_id. Some items are children to other items (for example an annotation can be a child-item on the article where it was made on which is the parent-item)

### Annotations
Annotations are the highlighted text of research materials. So, the most important information is in that annotated text content. To get that content and metadata about the annotations we can use the following queries.

To select all annotations of your library:

```sql
SELECT * FROM mindsdb_zotero.annotations
```

To select all data for one annotation by its id:
```sql
SELECT * FROM mindsdb_zotero.annotations WHERE item_id = "<item_id>"
```

To select all annotations of a parent item (like all annotations on a book):
```sql
SELECT * FROM mindsdb_zotero.annotations WHERE parent_item_id = "<parent_item_id>"
```



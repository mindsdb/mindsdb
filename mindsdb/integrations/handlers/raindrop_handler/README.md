# Raindrop.io Handler

Raindrop.io handler for MindsDB provides interfaces to connect to Raindrop.io via APIs and pull data into MindsDB. This handler also supports creating, updating, and deleting bookmarks and collections.

---

## Table of Contents

- [Raindrop.io Handler](#raindropio-handler)
  - [Table of Contents](#table-of-contents)
  - [About Raindrop.io](#about-raindropio)
  - [Raindrop.io Handler Implementation](#raindropio-handler-implementation)
  - [Raindrop.io Handler Initialization](#raindropio-handler-initialization)
  - [How to Get Your Raindrop.io API Key](#how-to-get-your-raindropio-api-key)
  - [Implemented Features](#implemented-features)
  - [Tables](#tables)
    - [Raindrops (Bookmarks)](#raindrops-bookmarks)
    - [Collections](#collections)
  - [Example Usage](#example-usage)
    - [Connecting to Raindrop.io](#connecting-to-raindropio)
    - [Selecting Bookmarks](#selecting-bookmarks)
    - [Creating Bookmarks](#creating-bookmarks)
    - [Updating Bookmarks](#updating-bookmarks)
    - [Deleting Bookmarks](#deleting-bookmarks)
    - [Working with Collections](#working-with-collections)

---

## About Raindrop.io

Raindrop.io is a bookmarking service that allows users to organize, save, and manage web bookmarks. It provides a clean interface for saving links, organizing them into collections, adding tags, notes, and highlights. Raindrop.io offers both personal and collaborative features for managing bookmarks across teams.

Website: https://raindrop.io

## Raindrop.io Handler Implementation

This handler was implemented using the [Raindrop.io REST API v1](https://developer.raindrop.io/). The handler provides comprehensive support for managing bookmarks (called "raindrops" in the API) and collections through SQL-like operations.

## Raindrop.io Handler Initialization

The Raindrop.io handler is initialized with the following parameter:

- `api_key`: a required Raindrop.io API access token

## How to Get Your Raindrop.io API Key

1. Sign up for an account on [Raindrop.io](https://raindrop.io)
2. Go to [App Management Console](https://app.raindrop.io/settings/integrations)
3. Create a new application or use an existing one
4. For testing purposes, you can copy the "Test token" from your application settings
5. For production use, implement OAuth2 flow as described in the [API documentation](https://developer.raindrop.io/v1/authentication/token)

## Implemented Features

### Raindrops (Bookmarks) Table
- [x] Support SELECT with advanced filtering and pagination
  - [x] Filter by collection_id, search terms, title
  - [x] Support for sorting by created, lastUpdate, sort, title
  - [x] Support for LIMIT and pagination
  - [x] Support for specific bookmark IDs
- [x] Support INSERT for creating new bookmarks
  - [x] Single bookmark creation
  - [x] Bulk bookmark creation
- [x] Support UPDATE for modifying existing bookmarks
  - [x] Single bookmark updates
  - [x] Bulk bookmark updates with WHERE conditions
- [x] Support DELETE for removing bookmarks
  - [x] Single bookmark deletion
  - [x] Bulk bookmark deletion with WHERE conditions

### Collections Table
- [x] Support SELECT with filtering and pagination
  - [x] Support for all collection fields
  - [x] Support for LIMIT and ORDER BY
- [x] Support INSERT for creating new collections
- [x] Support UPDATE for modifying existing collections
- [x] Support DELETE for removing collections
  - [x] Single collection deletion
  - [x] Bulk collection deletion

## Tables

### Raindrops (Bookmarks)

Available columns:
- `_id` (int): Unique bookmark ID
- `link` (str): The URL of the bookmark
- `title` (str): Title of the bookmark
- `excerpt` (str): Brief description or excerpt
- `note` (str): Personal notes
- `type` (str): Type of bookmark (link, article, image, video, etc.)
- `cover` (str): Cover image URL
- `tags` (str): Comma-separated tags
- `important` (bool): Whether the bookmark is marked as important
- `reminder` (datetime): Reminder date/time
- `removed` (bool): Whether the bookmark is removed/trashed
- `created` (datetime): Creation timestamp
- `lastUpdate` (datetime): Last update timestamp
- `domain` (str): Domain of the bookmarked URL
- `collection.id` (int): ID of the collection containing this bookmark
- `collection.title` (str): Title of the collection
- `user.id` (int): ID of the user who owns this bookmark
- `broken` (bool): Whether the link is broken
- `cache` (str): Whether a cached copy exists
- `file.name` (str): File name (for file bookmarks)
- `file.size` (int): File size (for file bookmarks)
- `file.type` (str): File type (for file bookmarks)

### Collections

Available columns:
- `_id` (int): Unique collection ID
- `title` (str): Collection title
- `description` (str): Collection description
- `color` (str): Collection color (hex code)
- `view` (str): View type (list, grid, etc.)
- `public` (bool): Whether the collection is public
- `sort` (int): Sort order
- `count` (int): Number of bookmarks in collection
- `created` (datetime): Creation timestamp
- `lastUpdate` (datetime): Last update timestamp
- `expanded` (bool): Whether the collection is expanded in UI
- `parent.id` (int): Parent collection ID (for nested collections)
- `user.id` (int): ID of the user who owns this collection
- `cover` (str): Cover image URL
- `access.level` (int): Access level
- `access.draggable` (bool): Whether the collection can be dragged

## Example Usage

### Connecting to Raindrop.io

```sql
CREATE DATABASE raindrop_db
WITH ENGINE = 'raindrop',
PARAMETERS = {
    "api_key": "your_raindrop_api_token_here"
};
```

### Selecting Bookmarks

```sql
-- Get all bookmarks
SELECT * FROM raindrop_db.raindrops;

-- Get bookmarks from a specific collection
SELECT * FROM raindrop_db.raindrops 
WHERE collection_id = 12345;

-- Search for bookmarks
SELECT title, link, tags FROM raindrop_db.raindrops 
WHERE search = 'programming' 
LIMIT 10;

-- Get bookmarks with specific tags
SELECT * FROM raindrop_db.raindrops 
WHERE title LIKE '%python%' 
ORDER BY created DESC;

-- Get important bookmarks
SELECT title, link, created FROM raindrop_db.raindrops 
WHERE important = true;
```

### Creating Bookmarks

```sql
-- Create a single bookmark
INSERT INTO raindrop_db.raindrops (link, title, note, tags, collection_id) 
VALUES (
    'https://example.com', 
    'Example Website', 
    'This is a great example', 
    'example,website,test',
    12345
);

-- Create multiple bookmarks
INSERT INTO raindrop_db.raindrops (link, title, collection_id) 
VALUES 
    ('https://github.com', 'GitHub', 0),
    ('https://stackoverflow.com', 'Stack Overflow', 0);
```

### Updating Bookmarks

```sql
-- Update a specific bookmark
UPDATE raindrop_db.raindrops 
SET title = 'New Title', note = 'Updated note', important = true
WHERE _id = 123456;

-- Update multiple bookmarks
UPDATE raindrop_db.raindrops 
SET collection_id = 54321 
WHERE tags LIKE '%oldtag%';

-- Mark bookmarks as important
UPDATE raindrop_db.raindrops 
SET important = true 
WHERE title LIKE '%important%';
```

### Deleting Bookmarks

```sql
-- Delete a specific bookmark
DELETE FROM raindrop_db.raindrops 
WHERE _id = 123456;

-- Delete bookmarks by search criteria
DELETE FROM raindrop_db.raindrops 
WHERE tags LIKE '%obsolete%';

-- Delete old bookmarks
DELETE FROM raindrop_db.raindrops 
WHERE created < '2023-01-01';
```

### Working with Collections

```sql
-- Get all collections
SELECT * FROM raindrop_db.collections;

-- Create a new collection
INSERT INTO raindrop_db.collections (title, description, color, view) 
VALUES ('Programming', 'Programming related bookmarks', '#FF0000', 'list');

-- Update a collection
UPDATE raindrop_db.collections 
SET title = 'Web Development', color = '#00FF00' 
WHERE _id = 12345;

-- Delete a collection
DELETE FROM raindrop_db.collections 
WHERE _id = 12345;

-- Get collections with bookmark counts
SELECT title, count, lastUpdate FROM raindrop_db.collections 
ORDER BY count DESC;
```

### Advanced Queries

```sql
-- Get bookmarks with collection information
SELECT r.title, r.link, r.tags, c.title as collection_name
FROM raindrop_db.raindrops r
JOIN raindrop_db.collections c ON r.collection_id = c._id
WHERE r.important = true;

-- Get recent bookmarks from multiple collections
SELECT title, link, created, collection_id
FROM raindrop_db.raindrops 
WHERE collection_id IN (123, 456, 789)
AND created > '2024-01-01'
ORDER BY created DESC 
LIMIT 20;

-- Search across title and notes
SELECT title, link, note 
FROM raindrop_db.raindrops 
WHERE title LIKE '%python%' OR note LIKE '%python%'
ORDER BY lastUpdate DESC;
```

## API Rate Limits

The Raindrop.io API has the following rate limits:
- 120 requests per minute for authenticated users
- The handler automatically handles pagination (API returns max 50 items per request)
- Bulk operations are used when possible to minimize API calls

## Error Handling

The handler includes comprehensive error handling:
- Connection validation on initialization
- Graceful fallback from bulk operations to individual operations when needed
- Proper error logging for debugging
- Handles API rate limiting and network errors

## Notes

- The `raindrops` table has an alias `bookmarks` for convenience
- All date fields are automatically converted to pandas datetime objects
- Tags are stored as comma-separated strings for easier querying
- The handler supports both single and bulk operations for better performance
- Collection ID 0 represents "All bookmarks" (unsorted)
- Collection ID -1 represents "Unsorted" bookmarks
- Collection ID -99 represents "Trash"

# MediaWiki Handler

MediaWiki handler for MindsDB provides interfaces to connect to the MediaWiki API and pull data into MindsDB.

---

## Table of Contents

- [MediaWiki Handler](#mediawiki-handler)
  - [Table of Contents](#table-of-contents)
  - [About MediaWiki](#about-githhub)
  - [MediaWiki Handler Implementation](#mediawiki-handler-implementation)
  - [MediaWiki Handler Initialization](#mediawiki-handler-initialization)
  - [Implemented Features](#implemented-features)
  - [Limitations](#limitations)
  - [TODO](#todo)
  - [Example Usage](#example-usage)

---

## About MediaWiki

MediaWiki is a free server-based wiki software, licensed under the GNU General Public License (GPL). It's designed to serve a website that gets millions of hits per day.
<br>
https://www.mediawiki.org/wiki/Manual:What_is_MediaWiki%3F

## MediaWiki Handler Implementation

This handler was implemented using [MediaWikiAPI](https://github.com/lehinevych/MediaWikiAPI), the Python wrapper for the MediaWiki API.

## MediaWiki Handler Initialization

The MediaWiki handler does not require any parameters to be initialized.

## Implemented Features

- [x] MediaWiki Pages Table
  - [x] Support SELECT
    - [x] Support LIMIT
    - [x] Support WHERE
    - [x] Support ORDER BY
    - [x] Support column selection

## Limitations
- Only the page ID, title, original title, content, summary, url and categories are returned for each page.
- WHERE clause only supports filtering by page ID and title.

Note: If a query is made without a WHERE clause, the handler will return 20 random pages.

## TODO
- [ ] Support INSERT, UPDATE and DELETE for Pages table
- [ ] Support more columns for Pages table

## Example Usage

The first step is to create a database with the new `mediawiki` engine:

~~~~sql
CREATE DATABASE mediawiki_datasource
WITH ENGINE = 'mediawiki'
~~~~

Use the established connection to query your database:

~~~~sql
SELECT * FROM mediawiki_datasource.pages
~~~~

Run more advanced queries:

~~~~sql
SELECT * 
FROM mediawiki_datasource.pages
WHERE title = 'Barack'
ORDER BY pageid
LIMIT 5
~~~~
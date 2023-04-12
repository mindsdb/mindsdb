# Spotify Handler

Spotify handler for MindsDB provides interfaces to connect to Spotify via APIs and pull repository data into MindsDB.

---

## Table of Contents

- [Spotify Handler](#spotify-handler)
  - [Table of Contents](#table-of-contents)
  - [About Spotify](#about-githhub)
  - [Spotify Handler Implementation](#spotify-handler-implementation)
  - [Spotify Handler Initialization](#spotify-handler-initialization)
  - [Implemented Features](#implemented-features)
  - [TODO](#todo)
  - [Example Usage](#example-usage)

---

## About Spotify

Spotify is an online music streaming service. It offers streaming of over 30 million songs (most of them are actually sound effects).

## Spotify Handler Implementation

## Spotify Handler Initialization

## Implemented Features


## TODO


## Example Usage

The first step is to create a database with the new `spotify` engine. 

~~~~sql
CREATE DATABASE mindsdb_spotify
WITH ENGINE = 'spotify',
PARAMETERS = {
  "repository": "mindsdb/mindsdb",
  "api_key": "your_api_key",    -- optional Spotify API key
};
~~~~

Use the established connection to query your database:

~~~~sql
SELECT * FROM mindsdb_spotify.issues
~~~~

Run more advanced queries:

~~~~sql
SELECT creator, title
  FROM mindsdb_spotify.playlists
  WHERE genre="reggae"
  ORDER BY created ASC
  LIMIT 10
~~~~
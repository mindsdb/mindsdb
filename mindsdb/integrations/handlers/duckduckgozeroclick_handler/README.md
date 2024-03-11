# DuckDuckGo Handler
This is the implementation of the DuckDuckGo handler for MindsDB.

## DuckDuckGo Zero-click 
DuckDuckGo Zero-click Info is packed with topic summaries, categories, disambiguation, official sites, !bang redirects, definitions, and more. It's a versatile API that serves various purposes, such as defining people, places, things, words, and concepts. Additionally, it offers direct links to other services through !bang syntax, lists related topics, and provides official sites whenever possible

## Implementation
This handler was implemented using the Rapid Api.

## Usage
In order to make use of this handler you need a Rapid api key:

```sql
CREATE DATABASE my_duckduckgo
WITH
  ENGINE = 'duckduckgo'
  PARAMETERS = {
    "api_key": "your_api_key_here"
  };
```

Now, you can use this established connection to query your database as follows:
```sql
SELECT * FROM my_duckduckgo("MindsDB");
```

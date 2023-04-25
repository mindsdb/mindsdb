# NewsAPI API Handler

This handler integrates with the [News API](https://newsapi.org/docs) to make aggregate article (data available to use for model training and predictions.

## Example: Selet artcles from news api

Connect to the NewsAPI API

We start by creating a database to connect to the News API.

```
CREATE DATABASE newsAPI
WITH
  ENGINE = 'newsAPI'
  PARAMETERS = {
	"api_key": "Your api key"
	};
```

### Select Data

To see if the connection was succesful, try searching for the most recent article data.

```
SELECT *
FROM newsAPI.article
WHERE query = 'mindsdb';
```

The result come with all these columns

* author
* title
* description
* url
* urlToImage
* publishedAt
* content
* source_id
* source_name
* query
* searchIn
* domains
* excludedDomains

You can select with multiple where clauses

```
SELECT *
FROM newsAPI.article
WHERE query = 'mindsdb'
AND sources="abc-news"
AND publishedAt >= "2023-03-23" AND  publishedAt <= "2023-04-23"
AND language = 'en'
ORDER BY publishedAt
LIMIT 40;
```

The query parameter in mandatory

You can check all available sources [here](https://newsapi.org/sources) .

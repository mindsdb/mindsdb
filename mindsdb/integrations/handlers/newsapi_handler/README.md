# NewsAPI API Handler

This handler integrates with the [News API](https://newsapi.org/docs) to make aggregate article (data available to use for model training and predictions.

## Example: Select articles from news api

Connect to the NewsAPI API

We start by creating a database to connect to the News API.

```
CREATE DATABASE newsAPI
WITH
  ENGINE = 'newsapi'
  PARAMETERS = {
	"api_key": "Your api key"
	};
```

### Select Data

To see if the connection was successful, try searching for the most recent article data.

```
SELECT *
FROM newsAPI.article
WHERE query = 'Python';
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

You can select with multiple clauses

```
SELECT *
FROM newsAPI.article
WHERE query = 'Python'
AND sources="bbc-news"
AND publishedAt >= "2021-03-23" AND  publishedAt <= "2023-04-23"
LIMIT 4;
```

#### **WHERE CLAUSE PARAMETERS:**

**query** : Base on the newsAPI documentation you must provide at least this  parameter with is the keywords or phrases to search for in the article title and body.

**sources** : Is a comma-seperated string of identifiers (maximum 20) for the news sources or blogs you want headlines from.

    You can check all available sources[here](https://newsapi.org/sources) .

**domains** : A comma-seperated string of domains (eg bbc.co.uk, techcrunch.com, engadget.com) to restrict the search to.

**exclude_domains** : A comma-seperated string of domains (eg bbc.co.uk, techcrunch.com, engadget.com) to remove from the results.

**searchIn** : The fields to restrict your query search to possible options are title, description,  content. Multiple options can be specified by separating them with a comma, for example: `title,content`

**lamguage** : The 2-letter ISO-639-1 code of the language you want to get headlines for. Possible options: `ar de, en es, fr he, it nl, no pt, ru,  sv, ud , zh`.

    Default: all languages returned.

**publishedAt** : A date and optional time for the oldest or newest article allowed.

#### **ORDER BY PARAMETERS:**

You can sort article by:

**relevancy** : articles more closely related to the query parameter come first

**popularity** : articles from popular sources and publishers come first

---
title: Web Crawler
sidebarTitle: Web Crawler
---

In this section, we present how to use a web crawler within MindsDB.

A web crawler is an automated script designed to systematically browse and index content on the internet. Within MindsDB, you can utilize a web crawler to efficiently collect data from various websites.

## Prerequisites

Before proceeding, ensure the following prerequisites are met:

1. Install MindsDB locally via [Docker](/setup/self-hosted/docker) or [Docker Desktop](/setup/self-hosted/docker-desktop).
2. To use Web Crawler with MindsDB, install the required dependencies following [this instruction](/setup/self-hosted/docker#install-dependencies).

## Connection

This handler does not require any connection parameters.

Here is how to initialize a web crawler:

```sql
CREATE DATABASE my_web 
WITH ENGINE = 'web';
```
<Tip>
The above query creates a database called `my_web`. This database by default has a table called `crawler` that we can use to crawl data from a given url/urls.
</Tip>

## Usage

<Note>
Specifying a `LIMIT` clause is required. To crawl all pages on a site, consider setting the limit to a high value, such as 10,000, which exceeds the expected number of pages. Be aware that setting a higher limit may result in longer response times.
</Note>

### Get Websites Content

The following usage examples demonstrate how to retrieve content from `docs.mindsdb.com`:

```sql
SELECT * 
FROM my_web.crawler 
WHERE url = 'docs.mindsdb.com' 
LIMIT 1;
```

You can also retrieve content from internal pages. The following query fetches the content from 10 internal pages:

```sql
SELECT * 
FROM my_web.crawler 
WHERE url = 'docs.mindsdb.com' 
LIMIT 10;
```

Another option is to get the content from multiple websites by using the `IN ()` operator:

```sql
SELECT * 
FROM my_web.crawler 
WHERE url IN ('docs.mindsdb.com', 'docs.python.org') 
LIMIT 1;
```

### Get PDF Content

MindsDB accepts [file uploads](/sql/create/file) of `csv`, `xlsx`, `xls`, `sheet`, `json`, and `parquet`. However, you can also configure the web crawler to fetch data from PDF files accessible via URLs.

```sql
SELECT * 
FROM my_web.crawler 
WHERE url = '<link-to-pdf-file>' 
LIMIT 1;
```
### Configuring Web Handler for Specific Domains

The Web Handler can be configured to interact only with specific domains by using the `web_crawling_allowed_sites` setting in the `config.json` file. 
This feature allows you to restrict the handler to crawl and process content only from the domains you specify, enhancing security and control over web interactions.

To configure this, simply list the allowed domains under the `web_crawling_allowed_sites` key in `config.json`. For example:

```json
"web_crawling_allowed_sites": [
    "https://docs.mindsdb.com",
    "https://another-allowed-site.com"
]
```

## Troubleshooting

<Warning>
`Web crawler encounters character encoding issues`

* **Symptoms**: Extracted text appears garbled or contains strange characters instead of the expected text.
* **Checklist**:
      1. Open a GitHub Issue: If you encounter a bug or a repeatable error with encoding, 
      report it on the [MindsDB GitHub](https://github.com/mindsdb/mindsdb/issues) repository by opening an issue.
</Warning>


<Warning>
`Web crawler times out while trying to fetch content`

* **Symptoms**: The crawler fails to retrieve data from a website, resulting in timeout errors.
* **Checklist**:
      1. Check the network connection to ensure the target site is reachable.
</Warning>
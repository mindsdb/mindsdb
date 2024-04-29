# Build your Web crawler

This integration allows you to query the results of a crawler in SQL:

- This can be particularly useful for building A/Q systems from data on a website.

Note that this crawler can crawl every single sub-site from the original.

Let's see in action

```sql
-- Should be able to create a web crawler database
CREATE DATABASE my_web 
With 
    ENGINE = 'web';
```

This creates a database called my_web. This database ships with a table called crawler that we can use to crawl data given some url/urls.


## Searching for web content in SQL

Let's get the content of a docs.mindsdb.com website:

```sql
SELECT 
   * 
FROM my_web.crawler 
WHERE 
   url = 'docs.mindsdb.com' 
LIMIT 1;
```


This should return the contents of docs.mindsdb.com.


Now, let's assume we want to search for the content on multiple websites.

```sql
SELECT 
   * 
FROM my_web.crawler 
WHERE 
   url IN ('docs.mindsdb.com', 'docs.python.org') 
LIMIT 30;
```

This command will crawl two sites and stop when the results count hits 30. The total count of rows in the result will be 30.

NOTE: limit is mandatory. If you want to crawl all pages on the site, you can pass a big number in the limit (for example, 10000), more than the expected count of pages on the site. 
However, a big limit also increases the time waiting for a response.



# Build your own Web crawler

This integration allows you to query the results of a crawler in SQL, 

- This can be particularly useful for building A/Q systems from data on website

Note that this crawler has the hability to crawl every single sub site from the original

Let's see in action

```
# Should be able to create a web crawler database
CREATE DATABASE my_web 
With 
    ENGINE = 'web';
```

This creates a database called my_web. This database ships with a table called crawler that we can use to crawl data given some url/urls.


## Searching for web conteant in SQL

Let's get the content of a website docs.mindsdb.com 

```
SELECT 
   * 
FROM my_web.crawler 
WHERE 
   url = 'docs.mindsdb.com' 
LIMIT 1;
```


This should return the contents of docs.mindsdb.com


Now let's assume that we want to search for the contents on more than one website


```
SELECT 
   * 
FROM my_web.crawler 
WHERE 
   url IN ('docs.mindsdb.com', 'docs.python.com') 
LIMIT 30;
```

This command will crawl two sites and stop when the count of results hit 30. The total count of rows in result will be 30.

NOTE: limit is mandatory. If you want to crawl all pages on site you can pass a big number in limit (for example 10000), more than expected count of pages on site. 
But big limit also increases time to waiting for response.



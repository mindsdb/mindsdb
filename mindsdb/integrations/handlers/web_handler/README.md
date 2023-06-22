# Build your own Web crawler

This integration allows you to query the results of a crawler in SQL, 

- This can be particularly useful for building A/Q systems from data on website

Note that this crawler has the hability to crawl every single sub site from the original

Let's see in action

```
# Should be able to create a web crawler database
CREATE DATABASE my_web 
With 
    ENGINE = 'webr';
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


This should return the conteants of docs.mindsdb.com


Now lets assume that we want to search for the contents on more than one website


```
SELECT 
   * 
FROM my_web.crawler 
WHERE 
   url IN ('docs.mindsdb.com', 'docs.python.com') 
LIMIT 1;
```

That should obtain two rows, LIMIT 1 limits how deep you go on each


now lets get 10 pages deep on both:


```
SELECT 
   * 
FROM my_web.crawler 
WHERE 
   url IN ('docs.mindsdb.com', 'docs.python.com') 
LIMIT 10;
```

You will get 20 results, as the crawler went 10 levels deeper into each url


NOTE: If you dont pass a limit, it will crawl until there are no more links to crawl, you should try it, it may take a while but it is pretty useful if say you wan to train a 
model to answer questions for you given the info on this website



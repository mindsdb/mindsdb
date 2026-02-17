# Google Search Console API Integration

This handler integrates with the [Google Search API](https://developers.google.com/webmaster-tools)
to allow you to use Google Search data in your SQL queries.

## Example: Predicting clicks based on Google Search activity

To see how the Google Search Console handler is used, let's walk through a simple example to create a model to predict
our clicks on a site based on Google Search activity.

## Connect to the Google Search API

We start by creating a database to connect to the Google Search Console API. Currently, there is no need for an API key:

However, you will need to have a Google account and have enabled the Google Search Console API.
Also, you will need to have the credentials in a json file. 
You can find more information on how to do
this [here](https://developers.google.com/webmaster-tools/v1/prereqs).

**Optional:**  The credentials file can be stored in the google_search handler folder in
the [mindsdb/integrations/google_search_handler](mindsdb/integrations/handlers/google_search_handler) directory.

~~~~sql
CREATE
DATABASE my_Search
WITH  ENGINE = 'google_search',
parameters = {
    'credentials': '/path/to/credentials.json'
};    
~~~~

This creates a database called my_Search. This database ships with a table called Analytics and with a table called Sitemaps that we can use to search for
Google Search data as well as to process Google Search data.

## Query your search traffic data with filters and parameters that you define

Let's get traffic data for a specific site.

~~~~sql
SELECT clicks
FROM my_console.Analytics
WHERE siteUrl = 'https://www.mindsdb.com'
  AND startDate = '2020-10-01'
  AND endDate = '2020-10-31'
  AND dimensions = 'query'
  AND type = 'web'
GROUP BY query
ORDER BY clicks
~~~~

## Submit a sitemap to Google Search Console

Let's test by submitting a sitemap to Google Search Console.

~~~~sql
INSERT INTO my_console.Sitemaps (siteUrl, sitemapUrl)
VALUES ('https://www.mindsdb.com', 'https://www.mindsdb.com/sitemap.xml')
~~~~

## Delete a sitemap from Google Search Console

Let's test by deleting a sitemap from Google Search Console.

~~~~sql
DELETE FROM my_console.Sitemaps
WHERE siteUrl = 'https://www.mindsdb.com'
  AND feedpath = 'https://www.mindsdb.com/sitemap.xml'
~~~~



## Creating a model to predict future clicks

Now we can incorporate external data from Google Search within our ML models.

~~~~sql
CREATE
PREDICTOR my_search_clicks
FROM my_search.Analytics
PREDICT
clicks
~~~~
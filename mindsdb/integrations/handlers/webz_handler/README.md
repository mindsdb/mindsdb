# Webz Handler

This handler integrates with the [Webz API](https://docs.webz.io/reference#1) to make
webz data available to use for model training, predictions and automations.



## Connect to the Webz API
The first step is to create a database with the new `webz` engine
by passing in the required `token` parameter:

```
CREATE DATABASE webz_datasource
WITH
  ENGINE = 'webz',
  PARAMETERS = {
    "token": "<webz active API key>"
};
```

## Querying news articles, blogs entries or open discussions

With the previous established connection, you can for instance,
query the 5 most relevant news articles, in english that contain
the text AI in the title

```
SELECT *
FROM webz_datasource.posts
WHERE query="language:english title:AI site_type:news"
ORDER BY posts.relevancy DESC
LIMIT 5;
```

The returned results should have rows like this:

| thread__uuid | thread__url | thread__site_full | thread__site | thread__site_section | thread__section_title | thread__title | thread__title_full | thread__published | thread__replies_count | thread__participants_count | thread__site_type | thread__main_image | thread__country | thread__site_categories | thread__social__facebook__likes | thread__social__facebook__shares | thread__social__facebook__comments | thread__social__gplus__shares | thread__social__pinterest__shares | thread__social__linkedin__shares | thread__social__stumbledupon__shares | thread__social__vk__shares | thread__performance_score | thread__domain_rank | thread__domain_rank_updated | thread__reach__per_million | thread__reach__page_views | thread__reach__updated | uuid | url | ord_in_thread | parent_url | author | published | title | text | language | external_links | external_images | rating | entities__persons | entities__organizations | entities__locations | crawled |
| ------------ | ----------- | ----------------- | ------------ | -------------------- | --------------------- | ------------- | ------------------ | ----------------- | --------------------- | -------------------------- | ----------------- | ------------------ | --------------- | ----------------------- | ------------------------------- | -------------------------------- | ---------------------------------- | ----------------------------- | --------------------------------- | -------------------------------- | ------------------------------------ | -------------------------- | ------------------------- | ------------------- | --------------------------- | -------------------------- | ------------------------- | ---------------------- | ---- | --- | ------------- | ---------- | ------ | --------- | ----- | ---- | -------- | -------------- | --------------- | ------ | ----------------- | ----------------------- | ------------------- | ------- |
| e893796adad8a85e6ab5202ac34b5791c8fbb017 | https://www.economist.com/business/2023/06/06/generative-ai-could-radically-alter-the-practice-of-law | www.economist.com | economist.com | http://feeds.feedburner.com/twitter.com/indiefulrok | BizToc | Generative AI could radically alter the practice of law | Generative AI could radically alter the practice of law | 2023-07-15T09:01:00.000+03:00 | 0 | 0 | news | https://c.biztoc.com/p/f96527e070f97968/s.webp | US | ["media","law_government_and_politics","politics"] | 2169 | 501 | 843 | 0 | 2 | 0 | 0 | 1 | 5 | 253 | 2023-07-11T13:16:20.000+03:00 | [NULL] | [NULL] | [NULL] | e893796adad8a85e6ab5202ac34b5791c8fbb017 | https://www.economist.com/business/2023/06/06/generative-ai-could-radically-alter-the-practice-of-law | 0 | [NULL] | [NULL] | 2023-07-15T09:01:00.000+03:00 | Generative AI could radically alter the practice of law | Generative AI could radically alter the practice of law economist.com/business/2023/06/06/generative-ai-could-radically-alter-the-practice-of-law L a conservative bunch, befitting a profession that rewards preparedness, sagacity and respect for precedent. No doubt many enjoyed a chuckle at the tale of Steven Schwartz, a personal-injury lawyer at the New York firm Levidow, Levidow & Oberman, who last month used Chat to help him prepare a court filing. He relied a bit too heavily on the artificial-intelligence ( )… This story appeared on | english | [] | [] | [NULL] | [{"name":"steven schwartz","sentiment":"none"}] | [{"name":"levidow, levidow & oberman","sentiment":"none"}] | [{"name":"new york","sentiment":"none"}] | 2023-07-15T09:52:32.226+03:00 |

## Queries reviews

You can also query the last 10 reviews crawled, in English,
with rating equal or higher than 4

```
SELECT *
FROM webz_datasource.reviews
WHERE query="language:english rating:>=4"
ORDER BY reviews.crawled ASC
LIMIT 10;
```

The returned results should have rows like this:

| item__uuid | item__url | item__site_full | item__site | item__site_section | item__section_title | item__title | item__title_full | item__published | item__reviews_count | item__reviewers_count | item__main_image | item__country | item__site_categories | item__domain_rank | item__domain_rank_updated | uuid | url | ord_in_thread | author | published | title | text | language | external_links | rating | crawled |
| ---------- | --------- | --------------- | ---------- | ------------------ | ------------------- | ----------- | ---------------- | --------------- | ------------------- | --------------------- | ---------------- | ------------- | --------------------- | ----------------- | ------------------------- | ---- | --- | ------------- | ------ | --------- | ----- | ---- | -------- | -------------- | ------ | ------- |

# Popularity Recommender ML Handler

Full docs can be found [here](https://docs.google.com/document/d/1kUlHZDdmbJqn0pUJRBJsaeulOzPXXJv7ALVUNmbZRJw/edit?usp=sharing)

## Briefly describe what ML framework does this handler integrate to MindsDB, and how?
This handler uses polars to create a simple but fast popularity recommender, recommending items based on global popularity and personal past interaction

## Why is this integration useful? What does the ideal predictive use case for this integration look like? When would you definitely not use this integration?

This integration is useful for a number of reasons
- Provides the most simple type of recommender, serving as a good benchmark for more complex recommenders
- It is very fast as it uses polars which is build on rust

The ideal use case is ecommerce rating data, web page browsing data or past purchase data for serving users recommendations

## Are models created with this integration fast and scalable, in general?
For the most part it is a scalable library and should support fairly large input datasets

## What are the recommended system specifications for models created with this framework?
The models will run on cpu

## To what degree can users control the underlying framework by passing parameters via the USING syntax?
Users are allowed complete control over the underlying framework by passing parameters via the USING syntax.

## Does this integration offer model explainability or insights via the DESCRIBE syntax?
No, this integration doesn't support DESCRIBE syntax.

## Does this integration support fine-tuning pre-existing models (i.e. is the update() method implemented)? Are there any caveats?
No, fine-tuning is not supported.

## Are there any other noteworthy aspects to this handler?
It is simple but fast


## Any directions for future work in subsequent versions of the handler?
tbc


## Please provide a minimal SQL example that uses this ML engine (pointers to integration tests in the PR also valid)
```sql
-- Create model, specifying input parameters
create model pop_rec_demo
from mysql_demo_db (SELECT * FROM movie_lens_ratings)
predict movieId
using
  engine='popularity_recommender',
  item_id='movieId', -- column name in input data that contains item_ids
  user_id='userId', -- column name in input data that contains user_ids
  n_recommendations=10 -- number of recommendations to return

-- Get recommendations per user based on the global most popular items
SELECT b.*
FROM mysql_demo_db.movie_lens_ratings a
join pop_rec_demo as b

-- Get recommendations for specific users based on popularity
SELECT b.*
FROM mysql_demo_db.movie_lens_ratings a
join pop_rec_demo as b
where a.userId in (215,216)
```

# LightFM ML Handler

Full docs can be found [here](https://docs.google.com/document/d/1kUlHZDdmbJqn0pUJRBJsaeulOzPXXJv7ALVUNmbZRJw/edit?usp=sharing)

## Briefly describe what ML framework does this handler integrate to MindsDB, and how?
This handler integrates [lightfm](https://github.com/lyst/lightfm) to mindsdb. This integration currently supports user-item and item-item collaborative filtering recommendations

## Why is this integration useful? What does the ideal predictive use case for this integration look like? When would you definitely not use this integration?

This integration is useful for a number of reasons
- Allow users to make use for the powerful lightfm recommendation framework library for performing recommendation on interaction data sets
- It supports user-item and item-item based recommendations

The ideal use case is ecommerce rating data, web page browsing data or past purchase data for serving users recommendations

## Are models created with this integration fast and scalable, in general?
For the most part it is a scalable library and should support fairly large input datasets

## What are the recommended system specifications for models created with this framework?
The models will run on cpu, the model utilises OpenMP which does not play nicely with Windows or OSX, it will default to single thread mode which will of course be slower.

It is therefore advisable to run via docker or linux to get the performance benefits of multiple threading with OpenMP

## To what degree can users control the underlying framework by passing parameters via the USING syntax?
Users are allowed complete control over the underlying framework by passing parameters via the USING syntax.

## Does this integration offer model explainability or insights via the DESCRIBE syntax?
Yes, this integration does support DESCRIBE syntax. This will provide info on performance metrics and sizes of user and item factors

## Does this integration support fine-tuning pre-existing models (i.e. is the update() method implemented)? Are there any caveats?
No, fine-tuning is not supported.

## Are there any other noteworthy aspects to this handler?
The handler only supports user-item and item-item recommendations
It also only support collaborative filtering


## Any directions for future work in subsequent versions of the handler?
- Support for hybrid recommender by allowing users to pass in user and item features in addition to interaction data
- Support for updating models with partial fit
- Support for user-user recommendations


## Example usage of lightFM Handler
```sql
-- Create lightfm engine
CREATE ML_ENGINE lightfm
FROM lightfm;

-- Create a lightfm model
create model lightfm_demo
from mysql_demo_db (SELECT * FROM movie_lens_ratings)
predict movieId
using
  engine='lightfm',
  item_id='movieId', -- column name in input data that contains item_ids
  user_id='userId', -- column name in input data that contains user_ids
  threshold=4, -- threshold if score of interaction is in input data
  n_recommendations=10, -- number of recommendations to return
  evaluation=true; -- OPTIONAL run evaluation, and store evaluation metrics. Defaults to false if not provided

-- Get recommendations for all item_item pairs
SELECT b.*
FROM lightfm_demo b
where recommender_type='item_item'

-- Get item-item recommendations for a specific item_id
SELECT b.* FROM lightfm_demo b where movieId = 100 USING recommender_type='item_item';

-- Get user-item recommendations for all user-item pairs
SELECT b.*
FROM lightfm_demo b
where recommender_type='user_item';

-- Get user-item recommendations for a specific user_id
SELECT b.* FROM lightfm_demo b where userId = 100 USING recommender_type='user_item';

-- Get user-item recommendations for a list user_ids
SELECT b.*
FROM mysql_demo_db.movie_lens_ratings a
join lightfm_demo as b
where a.userId in (215,216);

```

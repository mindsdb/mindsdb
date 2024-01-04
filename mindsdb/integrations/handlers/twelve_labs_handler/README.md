# Twelve Labs Handler

The Twelve Labs handler for MindsDB provides an interface to interact with the Twelve Labs API.

## About Twelve Labs
Powerful and seamless video search infrastructure for your application. 
<br>
https://twelvelabs.io/product

## Twelve Labs Handler Implementation

This handler was implemented using the standard [Requests](https://github.com/psf/requests) Python package.

## Creating an ML Engine

The first step to make use of this handler is to create an ML Engine. This can be done using the following syntax,
```sql
CREATE ML_ENGINE twelve_labs_engine
FROM twelve_labs
USING
  api_key = '<YOUR_API_KEY>';
```

## Creating Models

Now, you can use this ML Engine to create Models for the different tasks supported by the handler. 

When executing the `CREATE MODEL` statement, the following parameters are supported in the `USING` clause of the query:
- `engine`: The name of the ML Engine to use. This is a required parameter.
- `api_key`: The Twelve Labs API key to use for authentication. This is a required parameter.
- `task`: The task to perform. This is a required parameter and must be one of `search` or `classification`.
- `index_name`: The name of the index to use; if it does not exist, it will be created. This is a required parameter. More information about indexes can be found [here](https://docs.twelvelabs.io/docs/create-indexes).
- `index_options`: A list of the types of information within the video that will be processed by the video understanding engine. This is a required parameter and can be any combination of `visual`, `conversation`, `text_in_video` and `logo`. More information about index options can be found [here](https://docs.twelvelabs.io/docs/indexing-options).
- `video_urls`: A list of URLs to the videos to be indexed. This is an optional parameter, but if not specified, one of `video_files`, `video_urls_column` or `video_files_column` must be specified instead.
- `video_files`: A list of local paths to the videos to be indexed. This is an optional parameter, but if not specified, one of `video_urls`, `video_urls_column` or `video_files_column` must be specified instead.
- `video_urls_column`: The name of the column containing the URLs to the videos to be indexed. This is an optional parameter, but if not specified, one of `video_urls`, `video_files` or `video_files_column` must be specified instead.
- `video_files_column`: The name of the column containing the local paths to the videos to be indexed. This is an optional parameter, but if not specified, one of `video_urls`, `video_files` or `video_urls_column` must be specified instead.
- `search_options`: A list of the sources of information to use when performing a search. This parameter is required if the `task` is `search` and it should be a subset of `index_options`. More information about search options can be found [here](https://docs.twelvelabs.io/docs/search-options).
- `query_column`: The name of the column containing the search queries. This parameter is required if the `task` is `search`.

Given below are examples of creating Models for each of the supported tasks.

### Search
```sql
CREATE MODEL mindsdb.twelve_labs_search
PREDICT search_results
USING
  engine = 'twelve_labs_engine',
  api_key = '<YOUR_API_KEY>',
  task = 'search',
  index_name = 'index_1',
  index_options = ['visual', 'conversation', 'text_in_video', 'logo'],
  video_urls = ['https://.../video_1.mp4', 'https://.../video_2.mp4'],
  search_options = ['visual', 'conversation', 'text_in_video', 'logo']
  query_column = 'query';
```

As mentioned above, the `search_options` parameter is specific to the `search` task and should be a subset of `index_options`.

## Making Predictions

Once you have created a Model, you can use it to make predictions. 

Given below are examples of making predictions using Models created for each of the supported tasks.

### Search
```sql
SELECT *
FROM mindsdb.twelve_labs_search
WHERE query = 'search query';
```

Here, the `query` column is the name of the column containing the search queries as specified in the `query_column` parameter of the `CREATE MODEL` statement.

Note: At the moment, only a single query can be specified in the `WHERE` clause of the query. The `JOIN` clause for making multiple predictions will be added in a future release.
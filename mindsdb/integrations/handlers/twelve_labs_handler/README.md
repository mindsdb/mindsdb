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
  twelve_labs_api_key = '<YOUR_API_KEY>';
```

## Creating Models

Now, you can use this ML Engine to create Models for the different tasks supported by the handler. 

When executing the `CREATE MODEL` statement, the following parameters are supported in the `USING` clause of the query:
- `engine`: The name of the ML Engine to use. This is a required parameter.
- `twelve_labs_api_key`: The Twelve Labs API key to use for authentication, if the ML Engine is not provided.
- `base_url`: The base URL of the Twelve Labs API. This is an optional parameter and defaults to `https://api.twelvelabs.io/v1.2`. 
- `task`: The task to perform. This is a required parameter and must be one of `search` or `summarization`.
- `engine_id`: The ID of the Twelve Labs engine to use. This is an optional parameter and defaults to `marengo2.5`. However, certain tasks may require a different engine ID; for instance, the `summarization` task runs only on the Pegasus family of engines. More information about the different engines can be found [here](https://docs.twelvelabs.io/v1.2/docs/engine-options).
- `index_name`: The name of the index to use; if it does not exist, it will be created. This is a required parameter. More information about indexes can be found [here](https://docs.twelvelabs.io/docs/create-indexes).
- `index_options`: A list of the types of information within the video that will be processed by the video understanding engine. This is a required parameter and can be any combination of `visual`, `conversation`, `text_in_video` and `logo`. More information about index options can be found [here](https://docs.twelvelabs.io/docs/indexing-options). Certain engines support only a subset of these options; for instance, the Pegasus family of engines only support the `visual` and `conversation` options. More information about the about these configurations can be found [here](https://docs.twelvelabs.io/v1.2/docs/create-indexes).
- `video_urls`: A list of URLs to the videos to be indexed. This is an optional parameter, but if not specified, one of `video_files`, `video_urls_column` or `video_files_column` must be specified instead.
- `video_files`: A list of local paths to the videos to be indexed. This is an optional parameter, but if not specified, one of `video_urls`, `video_urls_column` or `video_files_column` must be specified instead.
- `video_urls_column`: The name of the column containing the URLs to the videos to be indexed. This is an optional parameter, but if not specified, one of `video_urls`, `video_files` or `video_files_column` must be specified instead.
- `video_files_column`: The name of the column containing the local paths to the videos to be indexed. This is an optional parameter, but if not specified, one of `video_urls`, `video_files` or `video_urls_column` must be specified instead.
- `search_options`: A list of the sources of information to use when performing a search. This parameter is required if the `task` is `search` and it should be a subset of `index_options`. More information about search options can be found [here](https://docs.twelvelabs.io/docs/search-options).
- `search_query_column`: The name of the column containing the search queries. This parameter is required if the `task` is `search`.
- `summarization_type`: The type of summarization to perform. This parameter is required if the `task` is `summarization` and it should be one of `summary`, `chapter` or `highlight`.
- `prompt` - Provide context for the summarization task, such as the target audience, style, tone of voice, and purpose. This is an optional parameter.

Given below are examples of creating Models for each of the supported tasks.

### Search
```sql
CREATE MODEL mindsdb.twelve_labs_search
PREDICT search_results
USING
  engine = 'twelve_labs_engine',
  task = 'search',
  index_name = 'index_1',
  index_options = ['visual', 'conversation', 'text_in_video', 'logo'],
  video_urls = ['https://.../video_1.mp4', 'https://.../video_2.mp4'],
  search_options = ['visual', 'conversation', 'text_in_video', 'logo']
  search_query_column = 'query';
```

As mentioned above, the `search_options` parameter is specific to the `search` task and should be a subset of `index_options`.

### Summarization

```sql
CREATE MODEL mindsdb.twelve_labs_summarization
PREDICT summarization_results
USING
  engine = 'twelve_labs_engine',
  task = 'summarization',
  engine_id = 'pegasus1',
  index_name = 'index_1',
  index_options = ['visual', 'conversation'],
  video_urls = ['https://.../video_1.mp4', 'https://.../video_2.mp4'],
  summarization_type = 'summary';
```

## Making Predictions

Given below are examples of using Models created for each of the supported tasks.


### Search
```sql
SELECT *
FROM mindsdb.twelve_labs_search
WHERE query = 'search query';
```

Here, the `query` column is the name of the column containing the search queries as specified in the `query_column` parameter of the `CREATE MODEL` statement.

Note: At the moment, only a single query can be specified in the `WHERE` clause of the query. The `JOIN` clause for making multiple predictions will be added in a future release.

### Summarization


```sql
SELECT *
FROM mindsdb.twelve_labs_summarization
WHERE video_id = 'video_1';
```

Here, the video IDs that were indexed by a model can be found by running a `DESCRIBE` statement on the it. The URL or file path of the video will be available in the `video_reference` column. The following is an example of how to run such a `DESCRIBE` statement,
```sql
DESCRIBE mindsdb.twelve_labs_summarization.indexed_videos;
```

The response returned will look something like this,
| video_id | created_at | updated_at | duration | engine_ids | filename | fps | height | size | video_reference | width |
| -------- | ---------- | ---------- | -------- | ---------- | -------- | --- | ------ | ---- | --------------- | ----- |
| 66c8425e35db9fa680cd4195 | 2024-02-23T03:39:10Z | 2024-02-23T03:39:12Z | 43.733333 | pegasus1 | test.mp4 | 30 | 1280 | 3737394 | /path/to/Videos/test.mp4 | 720 |

Note: This will display all of the indexed videos that are contained within the index specified in the `index_name` parameter of the `CREATE MODEL` statement. If the same index is used for multiple models, the `indexed_videos` table will contain all of the videos indexed by all of the models that use that index.
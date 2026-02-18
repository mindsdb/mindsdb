# Youtube Handler

Youtube handler for MindsDB provides interfaces to connect with Youtube via APIs and pull the video comments of the particular video.

## Youtube
Youtube is a social video sharing platform businesses and creators.  MindsDB users can deploy the youtube integration to perform NLP on youtube comments.

## Youtube Handler Initialization

The Youtube handler is initialized with the following parameters:

- `youtube_api_token`: Youtube API key to use for authentication 

Please follow this (link)[https://blog.hubspot.com/website/how-to-get-youtube-api-key] to generate the token for 
accessing youtube API

## Implemented Features

- [x] Youtube comments table 
  - [x] Support LIMIT
  - [x] Support WHERE
  - [x] Support ORDER BY
  - [x] Support column selection

- [x] Youtube channels table 

- [x] Youtube videos table 


## Example Usage

The first step is to create a database with the new `Youtube` engine.

~~~~sql
CREATE DATABASE mindsdb_youtube
WITH ENGINE = 'youtube',
PARAMETERS = {
  "youtube_api_token": "<your-youtube-api-key-token>"  
};
~~~~

Use the established connection to query the comments table 

~~~~sql
SELECT * FROM mindsdb_youtube.comments
WHERE video_id = "raWFGQ20OfA";
~~~~

Advanced queries for the youtube handler

~~~~sql
SELECT * FROM mindsdb_youtube.comments
WHERE video_id = "raWFGQ20OfA"
ORDER BY display_name ASC
LIMIT 5;
~~~~

Given a channel_id, get information about the channel

~~~~sql
SELECT * FROM mindsdb_youtube.channels
WHERE channel_id="UC-...";
~~~~

Here, `channel_id` column is mandatory in the where clause.

Get information about any youtube video using video_id:

~~~~sql
SELECT * FROM mindsdb_youtube.videos
WHERE video_id="id";
~~~~

`video_id` is a mandatory column in the where clause.

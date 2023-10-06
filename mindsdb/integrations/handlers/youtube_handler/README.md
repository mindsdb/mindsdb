# Youtube Handler

Youtube handler for MindsDB provides interfaces to connect with Youtube via APIs and pull the video comments of the particular video.

## Youtube
Youtube is app that needs no introduction. It provides a great distrbution for all business and creators and It opens-up a great opportunity to do NLP on youtube comments

## Youtube Handler Initialization

The Youtube handler is initialized with the following parameters:

- `youtube_api_token`: Youtube API key to use for authentication 

Please follow this (link)[https://blog.hubspot.com/website/how-to-get-youtube-api-key] to generate the token for accessing strava API

## Implemented Features

- [x] Youtube video_comments table 
  - [x] Support LIMIT
  - [x] Support WHERE
  - [x] Support ORDER BY
  - [x] Support column selection


## Example Usage

The first step is to create a database with the new `Youtube` engine.

~~~~sql
CREATE DATABASE mindsdb_youtube
WITH ENGINE = 'youtube',
PARAMETERS = {
  "youtube_api_token": "<your-youtube-api-key-token>"  
};
~~~~


Use the established connection to query the get_comments table 

~~~~sql
SELECT * FROM mindsdb_youtube.get_comments
WHERE youtube_video_id = "raWFGQ20OfA";
~~~~


Advanced queries for the youtube handler

~~~~sql
SELECT * FROM mindsdb_youtube.get_comments
WHERE youtube_video_id = "raWFGQ20OfA"
ORDER BY display_name ASC
LIMIT 5;
~~~~

Get information about any youtube video using video_id:

~~~~sql
select * from mindsdb_youtube.video where video_id="id"
~~~~

`video_id` is a mandatory column in the where clause.

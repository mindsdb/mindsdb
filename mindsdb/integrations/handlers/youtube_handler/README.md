# Youtube Handler

The Youtube Handler for MindsDB provides interfaces to connect with YouTube via APIs and pull video comments and video information from YouTube.

## YouTube
YouTube is a platform that needs no introduction. It provides great distribution for businesses and creators, and it opens up opportunities for natural language processing (NLP) on YouTube comments.

## Youtube Handler Initialization

The Youtube handler is initialized with the following parameter:

- `youtube_api_token`: YouTube API key for authentication. You can obtain this token by following [these instructions](https://blog.hubspot.com/website/how-to-get-youtube-api-key).

## Implemented Features

- [x] Youtube video_comments table
  - [x] Support LIMIT
  - [x] Support WHERE
  - [x] Support ORDER BY
  - [x] Support column selection
- [x] Youtube video_resource table
  - [x] Support LIMIT
  - [x] Support WHERE
  - [x] Support ORDER BY
  - [x] Support column selection

## Example Usage

### Create a Database

The first step is to create a database with the new `Youtube` engine. Replace `<your-youtube-api-key-token>` with your actual YouTube API key.

```sql
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

query the get_video_resource table
~~~~sql
SELECT * FROM mindsdb_youtube.get_video_resource
WHERE video_id = "<video-id>";
~~~~


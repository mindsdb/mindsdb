# Gong Handler

Gong handler for MindsDB provides interfaces to connect to Gong conversation intelligence platform via APIs and pull conversation data into MindsDB.

---

## Table of Contents

- [Gong Handler](#gong-handler)
  - [Table of Contents](#table-of-contents)
  - [About Gong](#about-gong)
  - [Gong Handler Implementation](#gong-handler-implementation)
  - [Gong Handler Initialization](#gong-handler-initialization)
  - [Implemented Features](#implemented-features)
  - [Example Usage](#example-usage)

---

## About Gong

Gong is a conversation intelligence platform that captures, analyzes, and provides insights from customer conversations. It helps sales teams understand customer interactions, improve sales performance, and make data-driven decisions.

The platform provides APIs for accessing call recordings, transcripts, analytics, and other conversation data.

## Gong Handler Implementation

This handler was implemented using the `requests` library to interact with the Gong REST API. The handler provides access to conversation data including calls, users, analytics, and transcripts.

## Gong Handler Initialization

The Gong handler is initialized with the following parameters:

- `api_key`: A required Gong API key for authentication
- `base_url`: An optional Gong API base URL (defaults to production)

Read about creating a Gong API key [here](https://app.gong.io/settings/api-keys).

## Implemented Features

- [x] Gong Calls Table
  - [x] Support SELECT
    - [x] Support LIMIT
    - [x] Support WHERE (date, user_id, call_type)
    - [x] Support ORDER BY (date, duration)
    - [x] Support column selection
- [x] Gong Users Table
  - [x] Support SELECT
    - [x] Support LIMIT
    - [x] Support WHERE (user_id, email)
    - [x] Support column selection
- [x] Gong Analytics Table
  - [x] Support SELECT
    - [x] Support LIMIT
    - [x] Support WHERE (call_id, sentiment_score, topic_score)
    - [x] Support column selection
- [x] Gong Transcripts Table
  - [x] Support SELECT
    - [x] Support LIMIT
    - [x] Support WHERE (call_id, text, speaker)
    - [x] Support text search in transcripts
    - [x] Support column selection

## Example Usage

The first step is to create a database with the new `gong` engine. You'll need a Gong API key to authenticate.

```sql
CREATE DATABASE gong_data
WITH ENGINE = 'gong',
PARAMETERS = {
  "api_key": "your_gong_api_key_here"
};
```

Use the established connection to query your database:

```sql
-- Get all calls
SELECT * FROM gong_data.calls LIMIT 10;
```

```sql
-- Get calls from a specific date range
SELECT * FROM gong_data.calls 
WHERE date >= '2024-01-01' AND date <= '2024-01-31'
ORDER BY date DESC
LIMIT 20;
```

```sql
-- Get users
SELECT * FROM gong_data.users LIMIT 10;
```

```sql
-- Get analytics for calls with high sentiment scores
SELECT * FROM gong_data.analytics 
WHERE sentiment_score > 0.7
LIMIT 10;
```

```sql
-- Search for specific text in transcripts
SELECT * FROM gong_data.transcripts 
WHERE text LIKE '%sales%' 
AND call_id = '12345';
```

```sql
-- Get transcripts for a specific call
SELECT speaker, timestamp, text 
FROM gong_data.transcripts 
WHERE call_id = '12345'
ORDER BY timestamp;
```

## API Endpoints

The handler connects to the following Gong API endpoints:

- `/v2/calls` - Access call recordings and metadata
- `/v2/users` - Get user information and permissions
- `/v2/analytics` - Access conversation analytics and insights
- `/v2/transcripts` - Get full conversation transcripts

## Data Schema

### Calls Table
- `call_id`: Unique identifier for the call
- `title`: Call title or description
- `date`: Call date and time
- `duration`: Call duration in seconds
- `recording_url`: URL to the call recording
- `call_type`: Type of call (e.g., "sales", "demo")
- `user_id`: ID of the user who made the call
- `participants`: Comma-separated list of participants
- `status`: Call status

### Users Table
- `user_id`: Unique identifier for the user
- `name`: User's full name
- `email`: User's email address
- `role`: User's role in the organization
- `permissions`: Comma-separated list of user permissions
- `status`: User status

### Analytics Table
- `call_id`: Reference to the call
- `sentiment_score`: Sentiment analysis score
- `topic_score`: Topic detection score
- `key_phrases`: Comma-separated list of key phrases
- `topics`: Comma-separated list of detected topics
- `emotions`: Comma-separated list of detected emotions
- `confidence_score`: Confidence score for the analysis

### Transcripts Table
- `call_id`: Reference to the call
- `speaker`: Name of the speaker
- `timestamp`: Timestamp of the transcript segment
- `text`: Transcribed text
- `confidence`: Confidence score for the transcription
- `segment_id`: Unique identifier for the transcript segment

## Error Handling

The handler includes comprehensive error handling for:
- Authentication failures
- API rate limiting
- Invalid API responses
- Network connectivity issues

## Rate Limiting

The handler respects Gong's API rate limits and includes appropriate delays between requests when necessary. 
"""
Constants and metadata for the Gong handler.
"""


def get_gong_api_info(handler_name: str) -> str:
    return f"""
        # Gong Handler Usage Guide: {handler_name}

        ## CRITICAL Performance Requirements

        **ALWAYS use date filters on analytics and (ideally) on calls/transcripts**
        - Analytics defaults to last 7 days if no date filters are provided
        - Calls/transcripts can be very large; add WHERE date ... or call_id filters
        - Example: WHERE c.date >= '2024-01-01' AND c.date < '2024-01-31'

        **ALWAYS use LIMIT with transcripts**
        - Transcripts can contain thousands of rows per call
        - Example: LIMIT 100

        **Query by call_id when fetching transcripts**
        - First get call IDs from `calls` (with date filters), then fetch transcripts for those IDs
        - Avoids massive data transfers

        ## Query Strategy by User Intent

        **"calls" / "meetings"** → query `{handler_name}.calls`

        **"sentiment" / "topics" / "what was discussed"** → query `{handler_name}.analytics` JOIN `{handler_name}.calls`
        - Analytics contains AI-generated insights about call content

        **"what did X say" / keyword search in speech** → query `{handler_name}.transcripts` JOIN `{handler_name}.calls`
        - Transcripts contain exact words spoken
        - Always filter by call_id and/or a narrow date range

        **"sales reps" / "users" / "team"** → query `{handler_name}.users`
        - Independent table, no JOIN needed

        ## Efficient JOIN Pattern

        Start with the smallest, filtered dataset first:
        ```sql
        SELECT c.title, a.sentiment_score
        FROM {handler_name}.calls c
        JOIN {handler_name}.analytics a ON c.call_id = a.call_id
        WHERE c.date >= '2024-01-01' AND c.date < '2024-02-01'
        LIMIT 50;
        ```
    """


GONG_TABLES_METADATA = {
    "calls": {
        "name": "calls",
        "type": "api_table",
        "description": "Call records from Gong with basic metadata including date, duration, participants, and status",
        "columns": [
            {"name": "call_id", "type": "str", "description": "Unique identifier for the call"},
            {"name": "title", "type": "str", "description": "Call title or subject"},
            {"name": "date", "type": "str", "description": "Call date (ISO 8601 format, YYYY-MM-DD)"},
            {"name": "duration", "type": "int", "description": "Call duration in seconds"},
            {"name": "recording_url", "type": "str", "description": "URL to the call recording (if available)"},
            {"name": "call_type", "type": "str", "description": "Call type/system classification"},
            {"name": "user_id", "type": "str", "description": "Primary user/owner of the call"},
            {"name": "participants", "type": "str", "description": "Comma-separated list of call participants"},
            {"name": "status", "type": "str", "description": "Call status (scheduled, completed, etc.)"},
        ],
        "filterable_columns": ["date", "status"],
        "api_endpoint": "/v2/calls",
        "supports_pagination": True,
    },
    "users": {
        "name": "users",
        "type": "api_table",
        "description": "User information including names, emails, roles, and permissions",
        "columns": [
            {"name": "user_id", "type": "str", "description": "Unique identifier for the user"},
            {"name": "name", "type": "str", "description": "User's full name"},
            {"name": "email", "type": "str", "description": "User's email address"},
            {"name": "role", "type": "str", "description": "User's role in the organization"},
            {"name": "permissions", "type": "str", "description": "User's permission levels"},
            {"name": "status", "type": "str", "description": "User status (active/inactive)"},
        ],
        "filterable_columns": ["email", "status"],
        "api_endpoint": "/v2/users",
        "supports_pagination": True,
    },
    "analytics": {
        "name": "analytics",
        "type": "api_table",
        "description": "Advanced call analytics including sentiment analysis, topics, key phrases, and interaction scores",
        "columns": [
            {"name": "call_id", "type": "str", "description": "Unique identifier for the call"},
            {"name": "sentiment_score", "type": "float", "description": "Overall sentiment score (0-1)"},
            {"name": "topic_score", "type": "float", "description": "Topic relevance score (0-1)"},
            {"name": "key_phrases", "type": "str", "description": "Comma-separated list of key phrases identified"},
            {"name": "topics", "type": "str", "description": "Comma-separated list of topics discussed"},
            {"name": "emotions", "type": "str", "description": "Emotional analysis metrics"},
            {"name": "confidence_score", "type": "str", "description": "AI confidence score for analytics"},
        ],
        "filterable_columns": ["date"],
        "api_endpoint": "/v2/calls/extensive",
        "supports_pagination": True,
        "notes": "Defaults to last 7 days if no date filters are provided.",
    },
    "transcripts": {
        "name": "transcripts",
        "type": "api_table",
        "description": "Call transcripts with speaker identification, timestamps, and confidence scores",
        "columns": [
            {"name": "call_id", "type": "str", "description": "Unique identifier for the call"},
            {"name": "speaker", "type": "str", "description": "Speaker identifier"},
            {"name": "timestamp", "type": "int", "description": "Timestamp in milliseconds from call start"},
            {"name": "text", "type": "str", "description": "Transcript text for this segment"},
            {"name": "confidence", "type": "float", "description": "Transcription confidence score"},
            {"name": "segment_id", "type": "str", "description": "Unique segment identifier"},
        ],
        "filterable_columns": ["call_id", "text"],
        "api_endpoint": "/v2/calls/transcript",
        "supports_pagination": True,
        "notes": "Fetch transcripts for specific call IDs; always filter by call_id and/or narrow date range.",
    },
}


GONG_PRIMARY_KEYS = {
    "calls": {"column_name": "call_id", "constraint_name": "pk_calls_call_id"},
    "users": {"column_name": "user_id", "constraint_name": "pk_users_user_id"},
    "analytics": {"column_name": "call_id", "constraint_name": "pk_analytics_call_id"},
    "transcripts": {"column_name": "segment_id", "constraint_name": "pk_transcripts_segment_id"},
}

GONG_FOREIGN_KEYS = {
    "analytics": [
        {
            "column_name": "call_id",
            "foreign_table_name": "calls",
            "foreign_column_name": "call_id",
            "constraint_name": "fk_analytics_call_id",
        }
    ],
    "transcripts": [
        {
            "column_name": "call_id",
            "foreign_table_name": "calls",
            "foreign_column_name": "call_id",
            "constraint_name": "fk_transcripts_call_id",
        }
    ],
}

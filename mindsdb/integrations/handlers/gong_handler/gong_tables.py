from typing import List, Dict, Any, Callable, Union
from datetime import datetime, timedelta, timezone
import pandas as pd

from mindsdb.integrations.libs.api_handler import APIResource
from mindsdb.integrations.utilities.sql_utils import FilterCondition, FilterOperator, SortColumn
from mindsdb.utilities import log


logger = log.getLogger(__name__)


def normalize_datetime_to_iso8601(dt_value: Union[str, datetime, None]) -> str:
    """
    Normalize various datetime formats to ISO 8601 with UTC timezone (Z suffix).

    Args:
        dt_value: Datetime value as string, datetime object, or None

    Returns:
        ISO 8601 formatted string with Z suffix (e.g., "2024-01-15T10:30:00Z")

    Raises:
        ValueError: If the datetime string cannot be parsed
    """
    if dt_value is None:
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    if isinstance(dt_value, datetime):
        # If naive (no timezone), assume UTC
        if dt_value.tzinfo is None:
            dt_value = dt_value.replace(tzinfo=timezone.utc)
        # Convert to UTC if not already
        elif dt_value.tzinfo != timezone.utc:
            dt_value = dt_value.astimezone(timezone.utc)
        return dt_value.strftime("%Y-%m-%dT%H:%M:%SZ")

    if isinstance(dt_value, str):
        dt_str = dt_value.strip()

        # Already in correct format with Z suffix
        if dt_str.endswith("Z") and "T" in dt_str:
            return dt_str

        # Has timezone offset like +00:00 or -05:00
        if dt_str.endswith(("00", "30")) and ("+" in dt_str[-6:] or dt_str[-6:-3] == "-"):
            try:
                dt_obj = datetime.fromisoformat(dt_str)
                return dt_obj.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            except ValueError:
                pass

        formats_to_try = [
            "%Y-%m-%dT%H:%M:%S",  # ISO without timezone
            "%Y-%m-%d %H:%M:%S",  # Common format
            "%Y-%m-%d",  # Date only (assume start of day UTC)
            "%Y/%m/%d",  # Alternative date format
            "%d-%m-%Y",  # European date format
            "%m/%d/%Y",  # US date format
        ]

        for fmt in formats_to_try:
            try:
                dt_obj = datetime.strptime(dt_str, fmt)
                dt_obj = dt_obj.replace(tzinfo=timezone.utc)
                return dt_obj.strftime("%Y-%m-%dT%H:%M:%SZ")
            except ValueError:
                continue

        try:
            dt_obj = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
            return dt_obj.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            raise ValueError(f"Unable to parse datetime string: {dt_str}")

    raise ValueError(f"Unsupported datetime type: {type(dt_value)}")


def paginate_api_call(
    api_call: Callable,
    result_key: str,
    limit: int = None,
    params: Dict[str, Any] = None,
    json_body: Dict[str, Any] = None,
    cursor_path: str = "records.cursor",
    cursor_param: str = "cursor",
    max_pages: int = 100,
) -> List[Dict]:
    """
    Helper function to paginate through API responses.

    Args:
        api_call: Function to call the API (should accept params and/or json)
        result_key: Key in response containing the data items
        limit: Maximum number of items to fetch
        params: Initial query parameters for GET requests
        json_body: Initial JSON body for POST requests
        cursor_path: Dot-notation path to cursor in response (e.g., "records.cursor")
        cursor_param: Parameter name for passing cursor to next request
        max_pages: Maximum number of pages to fetch (safety guard)

    Returns:
        List of items from paginated API calls
    """
    all_items = []
    seen_cursors = set()
    cursor = None
    page_count = 0

    params = params or {}
    json_body = json_body or {}

    while page_count < max_pages and (not limit or len(all_items) < limit):
        page_count += 1

        if cursor:
            if json_body:
                current_json = json_body.copy()
                current_json[cursor_param] = cursor
                response = api_call(json=current_json)
            else:
                current_params = params.copy()
                current_params[cursor_param] = cursor
                response = api_call(params=current_params)
        else:
            if json_body:
                response = api_call(json=json_body.copy())
            else:
                response = api_call(params=params.copy())

        items_batch = response.get(result_key, [])

        if not items_batch:
            break

        items_added_this_batch = 0
        for item in items_batch:
            if limit and len(all_items) >= limit:
                break

            item_str = str(sorted(item.items())) if isinstance(item, dict) else str(item)
            if item_str not in seen_cursors:
                all_items.append(item)
                seen_cursors.add(item_str)
                items_added_this_batch += 1

        if limit and len(all_items) >= limit:
            break

        next_cursor = response
        for key in cursor_path.split("."):
            next_cursor = next_cursor.get(key, {}) if isinstance(next_cursor, dict) else None
            if next_cursor is None:
                break

        if not next_cursor:
            break

        if next_cursor == cursor:
            logger.warning(f"API returned identical cursor: {cursor}. Stopping pagination.")
            break

        cursor_str = str(next_cursor)
        if cursor_str in seen_cursors:
            logger.warning(f"Detected cursor cycle at: {cursor_str}. Stopping pagination.")
            break

        seen_cursors.add(cursor_str)
        cursor = next_cursor

    if page_count >= max_pages:
        logger.warning(f"Reached maximum page limit ({max_pages}). There may be more data available.")

    return all_items


class GongCallsTable(APIResource):
    """The Gong Calls Table implementation"""

    def list(
        self,
        conditions: List[FilterCondition] = None,
        limit: int = None,
        sort: List[SortColumn] = None,
        targets: List[str] = None,
    ) -> pd.DataFrame:
        """Pulls data from the Gong Calls API

        Returns
        -------
        pd.DataFrame
            Gong calls matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        api_params = {}
        if conditions:
            for condition in conditions:
                if condition.column == "date" and condition.op == FilterOperator.GREATER_THAN:
                    api_params["fromDateTime"] = normalize_datetime_to_iso8601(condition.value)
                    condition.applied = True
                elif condition.column == "date" and condition.op == FilterOperator.LESS_THAN:
                    api_params["toDateTime"] = normalize_datetime_to_iso8601(condition.value)
                    condition.applied = True

        try:
            all_calls = paginate_api_call(
                api_call=lambda params: self.handler.call_gong_api("/v2/calls", params=params),
                result_key="calls",
                limit=limit,
                params=api_params,
            )

            data = []
            for call in all_calls:
                started = call.get("started", "")
                date = started.split("T")[0] if started else ""

                item = {
                    "call_id": call.get("id"),
                    "title": call.get("title"),
                    "date": date,
                    "duration": call.get("duration"),
                    "recording_url": call.get("url", ""),
                    "call_type": call.get("system"),
                    "user_id": call.get("primaryUserId"),
                    "participants": ",".join([p.get("name", "") for p in call.get("participants", [])]),
                    "status": call.get("status"),
                }
                data.append(item)

            df = pd.DataFrame(data)

            if conditions:
                for condition in conditions:
                    if not condition.applied and condition.column in df.columns:
                        if condition.op == FilterOperator.EQUAL:
                            df = df[df[condition.column] == condition.value]
                            condition.applied = True

            if sort:
                for col in sort:
                    if col.column in df.columns:
                        df = df.sort_values(by=col.column, ascending=col.ascending, na_position="last")
                        col.applied = True
                        break
            return df

        except Exception as e:
            logger.error(f"Error fetching calls from Gong API: {e}")
            raise

    def get_columns(self) -> List[str]:
        """Returns the columns of the calls table"""
        return [
            "call_id",
            "title",
            "date",
            "duration",
            "recording_url",
            "call_type",
            "user_id",
            "participants",
            "status",
        ]


class GongUsersTable(APIResource):
    """The Gong Users Table implementation"""

    def list(
        self,
        conditions: List[FilterCondition] = None,
        limit: int = None,
        sort: List[SortColumn] = None,
        targets: List[str] = None,
    ) -> pd.DataFrame:
        """Pulls data from the Gong Users API

        Returns
        -------
        pd.DataFrame
            Gong users matching the query
        """

        api_params = {}

        try:
            # Use pagination helper to fetch users
            all_users = paginate_api_call(
                api_call=lambda params: self.handler.call_gong_api("/v2/users", params=params),
                result_key="users",
                limit=limit,
                params=api_params,
            )

            # Process the limited data
            data = []
            for user in all_users:
                # Safely concatenate names - handle None values
                first_name = user.get("firstName") or ""
                last_name = user.get("lastName") or ""
                full_name = f"{first_name} {last_name}".strip()

                item = {
                    "user_id": user.get("id"),
                    "name": full_name,
                    "email": user.get("emailAddress", ""),
                    "role": user.get("title", ""),
                    "permissions": ",".join(user.get("permissions", [])),
                    "status": "active" if user.get("active", False) else "inactive",
                }
                data.append(item)

            df = pd.DataFrame(data)

            if conditions:
                for condition in conditions:
                    if condition.column in df.columns:
                        if condition.op == FilterOperator.EQUAL:
                            df = df[df[condition.column] == condition.value]
                            condition.applied = True

            if sort:
                for col in sort:
                    if col.column in df.columns:
                        df = df.sort_values(by=col.column, ascending=col.ascending, na_position="last")
                        col.applied = True
                        break

            if limit is not None:
                df = df.head(limit)

            return df

        except Exception as e:
            logger.error(f"Error fetching users from Gong API: {e}")
            raise

    def get_columns(self) -> List[str]:
        """Returns the columns of the users table"""
        return ["user_id", "name", "email", "role", "permissions", "status"]


class GongAnalyticsTable(APIResource):
    """The Gong Analytics Table implementation"""

    def list(
        self,
        conditions: List[FilterCondition] = None,
        limit: int = None,
        sort: List[SortColumn] = None,
        targets: List[str] = None,
    ) -> pd.DataFrame:
        """Pulls data from the Gong Analytics API

        Returns
        -------
        pd.DataFrame
            Gong analytics matching the query
        """

        try:
            # Default to last 7 days if no date filters provided
            default_from = datetime.now(timezone.utc) - timedelta(days=7)
            default_to = datetime.now(timezone.utc)

            payload = {
                "filter": {
                    "fromDateTime": normalize_datetime_to_iso8601(default_from),
                    "toDateTime": normalize_datetime_to_iso8601(default_to),
                },
                "contentSelector": {
                    "exposedFields": {
                        "content": {
                            "brief": True,
                            "outline": True,
                            "highlights": True,
                            "callOutcome": True,
                            "topics": True,
                            "trackers": True,
                        },
                        "interaction": {"personInteractionStats": True, "questions": True},
                    }
                },
            }

            if conditions:
                for condition in conditions:
                    if condition.column == "date" and condition.op == FilterOperator.GREATER_THAN:
                        payload["filter"]["fromDateTime"] = normalize_datetime_to_iso8601(condition.value)
                        condition.applied = True
                    elif condition.column == "date" and condition.op == FilterOperator.LESS_THAN:
                        payload["filter"]["toDateTime"] = normalize_datetime_to_iso8601(condition.value)
                        condition.applied = True

            # Fetch calls using improved pagination helper with POST support
            calls_data = paginate_api_call(
                api_call=lambda **kwargs: self.handler.call_gong_api("/v2/calls/extensive", method="POST", **kwargs),
                result_key="calls",
                limit=limit,
                json_body=payload,
                cursor_path="records.cursor",
                cursor_param="cursor",
                max_pages=100,
            )

            # Process each call to extract analytics
            all_analytics = []
            for call in calls_data:
                # Extract analytics from extensive call data
                content = call.get("content", {})
                interaction = call.get("interaction", {})
                metadata = call.get("metaData", {})

                # Sentiment and Emotion from InteractionStats
                person_stats = interaction.get("interactionStats", [])
                sentiment_score = 0
                emotions = ""  # Initialize emotions

                if person_stats:
                    stats_dict = {stat["name"]: stat["value"] for stat in interaction.get("interactionStats", [])}
                    sentiment_score = (
                        stats_dict.get("Talk Ratio", 0)
                        + stats_dict.get("Patience", 0)
                        + min(stats_dict.get("Interactivity", 0) / 10, 1.0)
                    ) / 3
                    emotions = f"Talk:{stats_dict.get('Talk Ratio', 0)}, Patience:{stats_dict.get('Patience', 0)}, Interactivity:{stats_dict.get('Interactivity', 0)}"

                # Topics from AI analysis
                topics = content.get("topics", [])
                topic_names = [
                    topic.get("name", "")
                    for topic in topics
                    if isinstance(topic, dict) and topic.get("duration", 0) > 0
                ]

                # Key phrases from AI
                trackers = content.get("trackers", [])
                key_phrases = [tracker.get("name", "") for tracker in trackers if tracker.get("count", 0) > 0]

                # Topic scoring based on relevance - prevent division by zero
                topic_score = 0
                if topics:
                    valid_topics = [topic for topic in topics if isinstance(topic, dict)]
                    if valid_topics:
                        total_topic_duration = sum([topic.get("duration", 0) for topic in valid_topics])
                        avg_topic_duration = total_topic_duration / len(valid_topics)
                        call_duration = metadata.get("duration", 0)
                        if call_duration > 0:
                            topic_score = avg_topic_duration / call_duration

                item = {
                    "call_id": metadata.get("id"),
                    "sentiment_score": round(sentiment_score, 3),
                    "topic_score": round(topic_score, 3),
                    "key_phrases": ", ".join(key_phrases),
                    "topics": ", ".join(topic_names),
                    "emotions": emotions,
                    "confidence_score": "",
                }
                all_analytics.append(item)

            df = pd.DataFrame(all_analytics)

            # Apply non-date filtering at DataFrame level
            if conditions:
                for condition in conditions:
                    if not condition.applied and condition.column in df.columns:
                        if condition.op == FilterOperator.EQUAL:
                            df = df[df[condition.column] == condition.value]
                            condition.applied = True

            # Apply sorting at DataFrame level
            if sort:
                for col in sort:
                    if col.column in df.columns:
                        df = df.sort_values(by=col.column, ascending=col.ascending, na_position="last")
                        col.applied = True
                        break

            if limit is not None:
                df = df.head(limit)

            return df

        except Exception as e:
            logger.error(f"Error fetching analytics from Gong API: {e}")
            raise

    def get_columns(self) -> List[str]:
        """Returns the columns of the analytics table"""
        return ["call_id", "sentiment_score", "topic_score", "key_phrases", "topics", "emotions", "confidence_score"]


class GongTranscriptsTable(APIResource):
    """The Gong Transcripts Table implementation"""

    def list(
        self,
        conditions: List[FilterCondition] = None,
        limit: int = None,
        sort: List[SortColumn] = None,
        targets: List[str] = None,
    ) -> pd.DataFrame:
        """Pulls data from the Gong Transcripts API

        Returns
        -------
        pd.DataFrame
            Gong transcripts matching the query
        """

        try:
            calls_api_params = {}

            if conditions:
                for condition in conditions:
                    if condition.column == "date" and condition.op == FilterOperator.GREATER_THAN:
                        calls_api_params["fromDateTime"] = normalize_datetime_to_iso8601(condition.value)
                        condition.applied = True
                    elif condition.column == "date" and condition.op == FilterOperator.LESS_THAN:
                        calls_api_params["toDateTime"] = normalize_datetime_to_iso8601(condition.value)
                        condition.applied = True

            # Fetch call IDs using pagination helper
            calls_fetch_limit = limit if limit else 100
            all_calls = paginate_api_call(
                api_call=lambda params: self.handler.call_gong_api("/v2/calls", params=params),
                result_key="calls",
                limit=calls_fetch_limit,
                params=calls_api_params,
            )
            all_call_ids = [call.get("id") for call in all_calls if call.get("id")]

            if not all_call_ids:
                return pd.DataFrame()
            call_ids_to_fetch = all_call_ids[:limit] if limit else all_call_ids

            # Fetch transcripts using improved pagination helper with POST support
            payload = {"filter": {"callIds": call_ids_to_fetch}}
            call_transcripts = paginate_api_call(
                api_call=lambda **kwargs: self.handler.call_gong_api("/v2/calls/transcript", method="POST", **kwargs),
                result_key="callTranscripts",
                limit=None,  # Get all transcripts for the specified calls
                json_body=payload,
                cursor_path="records.cursor",
                cursor_param="cursor",
                max_pages=50,
            )

            # Process transcripts
            all_transcript_data = []
            for call_transcript in call_transcripts:
                call_id = call_transcript.get("callId")
                transcript_segments = call_transcript.get("transcript", [])

                segment_counter = 0

                for speaker_block in transcript_segments:
                    speaker_id = speaker_block.get("speakerId")
                    sentences = speaker_block.get("sentences", [])

                    for sentence in sentences:
                        segment_counter += 1

                        item = {
                            "call_id": call_id,
                            "speaker": speaker_id,
                            "timestamp": sentence.get("start"),
                            "text": sentence.get("text"),
                            "confidence": sentence.get("confidence"),
                            "segment_id": f"{call_id}_{segment_counter}",
                        }
                        all_transcript_data.append(item)

            df = pd.DataFrame(all_transcript_data)

            if conditions:
                for condition in conditions:
                    if not condition.applied and condition.column in df.columns:
                        if condition.op == FilterOperator.EQUAL:
                            df = df[df[condition.column] == condition.value]
                            condition.applied = True
                        elif condition.op == FilterOperator.LIKE or condition.op == FilterOperator.CONTAINS:
                            if condition.column == "text":
                                df = df[df[condition.column].str.contains(condition.value, case=False, na=False)]
                                condition.applied = True

            if sort:
                for col in sort:
                    if col.column in df.columns:
                        df = df.sort_values(by=col.column, ascending=col.ascending, na_position="last")
                        col.applied = True
                        break

            if limit is not None:
                df = df.head(limit)
            return df

        except Exception as e:
            logger.error(f"Error fetching transcripts from Gong API: {e}")
            raise

    def get_columns(self) -> List[str]:
        """Returns the columns of the transcripts table"""
        return ["call_id", "speaker", "timestamp", "text", "confidence", "segment_id"]

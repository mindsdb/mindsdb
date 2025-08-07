from typing import List
from datetime import datetime, timedelta
import pandas as pd

from mindsdb.integrations.libs.api_handler import APIResource
from mindsdb.integrations.utilities.sql_utils import FilterCondition, FilterOperator, SortColumn
from mindsdb.utilities import log


logger = log.getLogger(__name__)


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
                    api_params["fromDateTime"] = condition.value
                    condition.applied = True
                elif condition.column == "date" and condition.op == FilterOperator.LESS_THAN:
                    api_params["toDateTime"] = condition.value
                    condition.applied = True

        try:
            all_calls = []
            cursor = None

            while True:
                current_params = api_params.copy()
                if cursor:
                    current_params["cursor"] = cursor

                response = self.handler.call_gong_api("/v2/calls", current_params)
                calls_batch = response.get("calls", [])

                for call in calls_batch:
                    if limit and len(all_calls) >= limit:
                        break
                    all_calls.append(call)

                records_info = response.get("records", {})
                if (limit and len(all_calls) >= limit) or "cursor" not in records_info:
                    break

                cursor = records_info.get("cursor")
                if not cursor:
                    break

            # Process the limited data
            data = []
            for call in all_calls:
                item = {
                    "call_id": call.get("id"),
                    "title": call.get("title"),
                    "date": call.get("started").split("T")[0],
                    "duration": call.get("duration"),
                    "recording_url": call.get("url", ""),
                    "call_type": call.get("system"),
                    "user_id": call.get("primaryUserId"),
                    "participants": ",".join([p.get("name", "") for p in call.get("participants", [])]),
                    "status": call.get("status"),
                }
                data.append(item)

            df = pd.DataFrame(data)

            # Apply non-date filtering at DataFrame level
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

            if limit is not None:
                df = df.head(limit)

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
            all_users = []
            cursor = None

            while True:
                current_params = api_params.copy()
                if cursor:
                    current_params["cursor"] = cursor

                response = self.handler.call_gong_api("/v2/users", current_params)
                users_batch = response.get("users", [])

                for user in users_batch:
                    if limit and len(all_users) >= limit:
                        break
                    all_users.append(user)

                records_info = response.get("records", {})
                if (limit and len(all_users) >= limit) or "cursor" not in records_info:
                    break

                cursor = records_info.get("cursor")
                if not cursor:
                    break

            # Process the limited data
            data = []
            for user in all_users:
                item = {
                    "user_id": user.get("id"),
                    "name": user.get("firstName") + " " + user.get("lastName"),
                    "email": user.get("emailAddress"),
                    "role": user.get("title"),
                    "permissions": ",".join(user.get("permissions", [])),
                    "status": "active" if user.get("active") else "inactive",
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
            payload = {
                "filter": {
                    "fromDateTime": (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "toDateTime": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
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
                        payload["filter"]["fromDateTime"] = condition.value
                        condition.applied = True
                    elif condition.column == "date" and condition.op == FilterOperator.LESS_THAN:
                        payload["filter"]["toDateTime"] = condition.value
                        condition.applied = True

            session = self.handler.connect()

            all_analytics = []
            cursor = None

            while True:
                current_payload = payload.copy()
                if cursor:
                    current_payload["cursor"] = cursor

                response = session.post(f"{self.handler.base_url}/v2/calls/extensive", json=current_payload)
                response.raise_for_status()
                calls_response = response.json()

                analytics_batch = calls_response.get("calls", [])

                # Process and add analytics
                for call in analytics_batch:
                    if limit and len(all_analytics) >= limit:
                        break

                    # Extract analytics from extensive call data
                    content = call.get("content", {})
                    interaction = call.get("interaction", {})
                    metadata = call.get("metaData", {})

                    # Sentiment and Emotion from InteractionStats
                    person_stats = interaction.get("interactionStats", [])
                    sentiment_score = 0
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

                    # Topic scoring based on relevance
                    topic_duration = (
                        sum([topic.get("duration", 0) for topic in topics if isinstance(topic, dict)]) / len(topics)
                        if topics
                        else 0
                    )
                    call_duration = metadata.get("duration", 1)
                    topic_score = (topic_duration / call_duration) if call_duration > 0 else 0

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

                records_info = calls_response.get("records", {})
                if (limit and len(all_analytics) >= limit) or "cursor" not in records_info:
                    break

                cursor = records_info.get("cursor")
                if not cursor:
                    break

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
                        calls_api_params["fromDateTime"] = condition.value
                        condition.applied = True
                    elif condition.column == "date" and condition.op == FilterOperator.LESS_THAN:
                        calls_api_params["toDateTime"] = condition.value
                        condition.applied = True

            calls_fetch_limit = limit if limit else 100

            all_call_ids = []
            cursor = None

            while len(all_call_ids) < calls_fetch_limit:
                current_params = calls_api_params.copy()
                if cursor:
                    current_params["cursor"] = cursor

                calls_response = self.handler.call_gong_api("/v2/calls", current_params)
                calls_batch = calls_response.get("calls", [])

                batch_call_ids = [call.get("id") for call in calls_batch if call.get("id")]
                all_call_ids.extend(batch_call_ids)

                records_info = calls_response.get("records", {})
                if len(all_call_ids) >= calls_fetch_limit or "cursor" not in records_info:
                    break

                cursor = records_info.get("cursor")
                if not cursor:
                    break

            if not all_call_ids:
                return pd.DataFrame()
            call_ids_to_fetch = all_call_ids[:limit] if limit else all_call_ids

            session = self.handler.connect()
            all_transcript_data = []
            transcript_cursor = None

            while True:
                payload = {"filter": {"callIds": call_ids_to_fetch}}

                if transcript_cursor:
                    payload["cursor"] = transcript_cursor

                response = session.post(f"{self.handler.base_url}/v2/calls/transcript", json=payload)
                response.raise_for_status()
                transcript_response = response.json()
                transcript_batch = transcript_response.get("callTranscripts", [])

                for call_transcript in transcript_batch:
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

                transcript_records_info = transcript_response.get("records", {})
                if "cursor" not in transcript_records_info:
                    break

                transcript_cursor = transcript_records_info.get("cursor")
                if not transcript_cursor:
                    break

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

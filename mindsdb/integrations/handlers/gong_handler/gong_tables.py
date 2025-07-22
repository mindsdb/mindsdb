from typing import List
from datetime import datetime, timedelta
import pandas as pd

from mindsdb.integrations.libs.api_handler import APIResource
from mindsdb.integrations.utilities.sql_utils import (
    FilterCondition, FilterOperator, SortColumn
)
from mindsdb.utilities import log


logger = log.getLogger(__name__)


class GongCallsTable(APIResource):
    """The Gong Calls Table implementation"""

    def list(self,
             conditions: List[FilterCondition] = None,
             limit: int = None,
             sort: List[SortColumn] = None,
             targets: List[str] = None) -> pd.DataFrame:
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

        # Default parameters for the API call
        api_params = {
            'limit': limit,
            'offset': 0
        }

        if limit is not None:
            api_params['limit'] = limit
        else:
            limit = 20

        # Handle date filtering
        if conditions:
            for condition in conditions:
                if condition.column == 'date' and condition.op == FilterOperator.GREATER_THAN:
                    api_params['fromDateTime'] = condition.value
                    condition.applied = True
                elif condition.column == 'date' and condition.op == FilterOperator.LESS_THAN:
                    api_params['toDateTime'] = condition.value
                    condition.applied = True
                elif condition.column == 'user_id' and condition.op == FilterOperator.EQUAL:
                    api_params['userId'] = condition.value
                    condition.applied = True
                elif condition.column == 'call_type' and condition.op == FilterOperator.EQUAL:
                    api_params['callType'] = condition.value
                    condition.applied = True

        # Handle sorting
        if sort:
            for col in sort:
                if col.column in ('date', 'duration'):
                    api_params['sortBy'] = col.column
                    api_params['sortOrder'] = 'asc' if col.ascending else 'desc'
                    col.applied = True
                    break

        try:
            # Make API call to get calls
            response = self.handler.call_gong_api('/v2/calls', api_params)
            calls_data = response.get('calls', [])

            data = []
            for call in calls_data:
                item = {
                    'call_id': call.get('id'),
                    'title': call.get('title'),
                    'date': call.get('date'),
                    'duration': call.get('duration'),
                    'recording_url': call.get('recordingUrl'),
                    'call_type': call.get('callType'),
                    'user_id': call.get('userId'),
                    'participants': ','.join([p.get('name', '') for p in call.get('participants', [])]),
                    'status': call.get('status')
                }
                data.append(item)

            return pd.DataFrame(data)

        except Exception as e:
            logger.error(f"Error fetching calls from Gong API: {e}")
            raise

    def get_columns(self) -> List[str]:
        """Returns the columns of the calls table"""
        return [
            'call_id',
            'title',
            'date',
            'duration',
            'recording_url',
            'call_type',
            'user_id',
            'participants',
            'status'
        ]


class GongUsersTable(APIResource):
    """The Gong Users Table implementation"""

    def list(self,
             conditions: List[FilterCondition] = None,
             limit: int = None,
             sort: List[SortColumn] = None,
             targets: List[str] = None) -> pd.DataFrame:
        """Pulls data from the Gong Users API

        Returns
        -------
        pd.DataFrame
            Gong users matching the query
        """
        api_params = {
            'limit': limit,
            'offset': 0
        }

        if limit is not None:
            api_params['limit'] = limit
        else:
            limit = 20

        # Handle filtering
        if conditions:
            for condition in conditions:
                if condition.column == 'user_id' and condition.op == FilterOperator.EQUAL:
                    api_params['userId'] = condition.value
                    condition.applied = True
                elif condition.column == 'email' and condition.op == FilterOperator.EQUAL:
                    api_params['email'] = condition.value
                    condition.applied = True

        try:
            # Make API call to get users
            response = self.handler.call_gong_api('/v2/users', api_params)
            users_data = response.get('users', [])

            data = []
            for user in users_data:
                item = {
                    'user_id': user.get('id'),
                    'name': user.get('name'),
                    'email': user.get('email'),
                    'role': user.get('role'),
                    'permissions': ','.join(user.get('permissions', [])),
                    'status': user.get('status')
                }
                data.append(item)

            return pd.DataFrame(data)

        except Exception as e:
            logger.error(f"Error fetching users from Gong API: {e}")
            raise

    def get_columns(self) -> List[str]:
        """Returns the columns of the users table"""
        return [
            'user_id',
            'name',
            'email',
            'role',
            'permissions',
            'status'
        ]


class GongAnalyticsTable(APIResource):
    """The Gong Analytics Table implementation"""

    def list(self,
             conditions: List[FilterCondition] = None,
             limit: int = None,
             sort: List[SortColumn] = None,
             targets: List[str] = None) -> pd.DataFrame:
        """Pulls data from the Gong Analytics API

        Returns
        -------
        pd.DataFrame
            Gong analytics matching the query
        """
        api_params = {
            'limit': limit,
            'offset': 0
        }

        if limit is not None:
            api_params['limit'] = limit
        else:
            limit = 20

        try:
            session = self.handler.connect()

            payload = {
                'filter': {
                    'fromDateTime': (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%dT%H:%M:%SZ'),
                    'toDateTime': datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
                },
                'contentSelector': {
                    'exposedFields': {
                        'content': {
                            'brief': True,
                            'outline': True,
                            'highlights': True,
                            'callOutcome': True,
                            'topics': True,
                            'trackers': True
                        },
                        'interaction': {
                            'personInteractionStats': True,
                            'questions': True
                        }
                    }
                }
            }

            response = session.post(f"{self.handler.base_url}/v2/calls/extensive", json=payload)
            response.raise_for_status()
            calls_response = response.json()

            analytics_data = calls_response.get('calls', [])

            data = []
            for call in analytics_data:
                # Extract real analytics from extensive call data
                content = call.get('content', {})
                interaction = call.get('interaction', {})

                # Sentiment from personInteractionStats
                person_stats = interaction.get('personInteractionStats', [])
                sentiment_score = 0
                if person_stats:
                    sentiments = [stat.get('sentiment', 0) for stat in person_stats if stat.get('sentiment') is not None]
                    sentiment_score = sum(sentiments) / len(sentiments) if sentiments else 0

                # Topics from AI analysis
                topics = content.get('topics', [])
                topic_names = [topic.get('name', '') for topic in topics if isinstance(topic, dict)]

                # Key phrases from AI
                trackers = content.get('trackers', [])
                key_phrases = [tracker.get('name', '') for tracker in trackers if isinstance(tracker, dict)]

                # Call outcome confidence
                call_outcome = content.get('callOutcome', {})
                confidence_score = call_outcome.get('confidence', 0) if isinstance(call_outcome, dict) else 0

                # Topic scoring based on relevance
                topic_score = sum([topic.get('score', 0) for topic in topics if isinstance(topic, dict)]) / len(topics) if topics else 0

                item = {
                    'call_id': call.get('id'),
                    'sentiment_score': round(sentiment_score, 3),
                    'topic_score': round(topic_score, 3),
                    'key_phrases': ', '.join(key_phrases[:5]),
                    'topics': ', '.join(topic_names[:5]),
                    'emotions': '',
                    'confidence_score': round(confidence_score, 3)
                }
                data.append(item)

            return pd.DataFrame(data)

        except Exception as e:
            logger.error(f"Error fetching analytics from Gong API: {e}")
            raise

    def get_columns(self) -> List[str]:
        """Returns the columns of the analytics table"""
        return [
            'call_id',
            'sentiment_score',
            'topic_score',
            'key_phrases',
            'topics',
            'emotions',
            'confidence_score'
        ]


class GongTranscriptsTable(APIResource):
    """The Gong Transcripts Table implementation"""

    def list(self,
             conditions: List[FilterCondition] = None,
             limit: int = None,
             sort: List[SortColumn] = None,
             targets: List[str] = None) -> pd.DataFrame:
        """Pulls data from the Gong Transcripts API

        Returns
        -------
        pd.DataFrame
            Gong transcripts matching the query
        """
        api_params = {
            'limit': limit,
            'offset': 0
        }
        if limit is not None:
            api_params['limit'] = limit
        else:
            limit = 20

        try:
            # First get recent calls
            calls_response = self.handler.call_gong_api('/v2/calls', {'limit': limit})
            call_ids = [call.get('id') for call in calls_response.get('calls', []) if call.get('id')]

            if not call_ids:
                return pd.DataFrame()

            # Get transcripts
            session = self.handler.connect()
            payload = {
                'filter': {
                    'callIds': call_ids
                }
            }
            response = session.post(f"{self.handler.base_url}/v2/calls/transcript", json=payload)
            response.raise_for_status()
            transcript_response = response.json()
            transcript_data = transcript_response.get('callTranscripts', [])

            data = []
            for call_transcript in transcript_data:
                call_id = call_transcript.get('callId')
                transcript_segments = call_transcript.get('transcript', [])

                for segment in transcript_segments:
                    item = {
                        'call_id': call_id,
                        'speaker': segment.get('speakerId'),
                        'timestamp': segment.get('startTime'),
                        'text': segment.get('text'),
                        'confidence': segment.get('confidence'),
                        'segment_id': segment.get('segmentId')
                    }
                    data.append(item)

            return pd.DataFrame(data)

        except Exception as e:
            logger.error(f"Error fetching transcripts from Gong API: {e}")
            raise

    def get_columns(self) -> List[str]:
        """Returns the columns of the transcripts table"""
        return [
            'call_id',
            'speaker',
            'timestamp',
            'text',
            'confidence',
            'segment_id'
        ]

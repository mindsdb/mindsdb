from typing import Dict, List, Text, Optional
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

        if limit is None:
            limit = 20

        # Default parameters for the API call
        api_params = {
            'limit': limit,
            'offset': 0
        }

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
                    sort.applied = True
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

        if limit is None:
            limit = 20

        api_params = {
            'limit': limit,
            'offset': 0
        }

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

        if limit is None:
            limit = 20

        api_params = {
            'limit': limit,
            'offset': 0
        }

        # Handle filtering
        if conditions:
            for condition in conditions:
                if condition.column == 'call_id' and condition.op == FilterOperator.EQUAL:
                    api_params['callId'] = condition.value
                    condition.applied = True
                elif condition.column == 'sentiment_score' and condition.op == FilterOperator.GREATER_THAN:
                    api_params['minSentimentScore'] = condition.value
                    condition.applied = True
                elif condition.column == 'topic_score' and condition.op == FilterOperator.GREATER_THAN:
                    api_params['minTopicScore'] = condition.value
                    condition.applied = True

        try:
            # Make API call to get analytics
            response = self.handler.call_gong_api('/v2/analytics', api_params)
            analytics_data = response.get('analytics', [])

            data = []
            for analytics in analytics_data:
                item = {
                    'call_id': analytics.get('callId'),
                    'sentiment_score': analytics.get('sentimentScore'),
                    'topic_score': analytics.get('topicScore'),
                    'key_phrases': ','.join(analytics.get('keyPhrases', [])),
                    'topics': ','.join(analytics.get('topics', [])),
                    'emotions': ','.join(analytics.get('emotions', [])),
                    'confidence_score': analytics.get('confidenceScore')
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

        if limit is None:
            limit = 20

        api_params = {
            'limit': limit,
            'offset': 0
        }

        # Handle filtering
        if conditions:
            for condition in conditions:
                if condition.column == 'call_id' and condition.op == FilterOperator.EQUAL:
                    api_params['callId'] = condition.value
                    condition.applied = True
                elif condition.column == 'text' and condition.op == FilterOperator.LIKE:
                    api_params['searchText'] = condition.value.replace('%', '')
                    condition.applied = True
                elif condition.column == 'speaker' and condition.op == FilterOperator.EQUAL:
                    api_params['speaker'] = condition.value
                    condition.applied = True

        try:
            # Make API call to get transcripts
            response = self.handler.call_gong_api('/v2/transcripts', api_params)
            transcripts_data = response.get('transcripts', [])

            data = []
            for transcript in transcripts_data:
                item = {
                    'call_id': transcript.get('callId'),
                    'speaker': transcript.get('speaker'),
                    'timestamp': transcript.get('timestamp'),
                    'text': transcript.get('text'),
                    'confidence': transcript.get('confidence'),
                    'segment_id': transcript.get('segmentId')
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
from typing import List

from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.utilities import log

from mindsdb_sql_parser import ast
from mindsdb.integrations.utilities.handlers.query_utilities import (
    SELECTQueryParser,
    SELECTQueryExecutor,
    INSERTQueryParser,
)

import pandas as pd
import re
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api.formatters import JSONFormatter

logger = log.getLogger(__name__)


class YoutubeCommentsTable(APITable):
    """Youtube List Comments  by video id Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the youtube  "commentThreads()" API endpoint
        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query
        Returns
        -------
        pd.DataFrame
            youtube "commentThreads()" matching the query
        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """
        select_statement_parser = SELECTQueryParser(query, "comments", self.get_columns())

        (
            selected_columns,
            where_conditions,
            order_by_conditions,
            result_limit,
        ) = select_statement_parser.parse_query()

        channel_id, video_id = None, None
        for a_where in where_conditions:
            if a_where[1] == "video_id":
                if a_where[0] != "=":
                    raise NotImplementedError("Only '=' operator is supported for video_id column.")
                else:
                    video_id = a_where[2]
            elif a_where[1] == "channel_id":
                if a_where[0] != "=":
                    raise NotImplementedError("Only '=' operator is supported for channel_id column.")
                else:
                    channel_id = a_where[2]

        if not video_id and not channel_id:
            raise ValueError("Either video_id or channel_id has to be present in where clause.")

        comments_df = self.get_comments(video_id=video_id, channel_id=channel_id)

        select_statement_executor = SELECTQueryExecutor(
            comments_df,
            selected_columns,
            [
                where_condition
                for where_condition in where_conditions
                if where_condition[1] not in ["video_id", "channel_id"]
            ],
            order_by_conditions,
            result_limit if query.limit else None,
        )

        comments_df = select_statement_executor.execute_query()

        return comments_df

    def insert(self, query: ast.Insert) -> None:
        """Inserts data into the YouTube POST /commentThreads API endpoint.

        Parameters
        ----------
        query : ast.Insert
            Given SQL INSERT query

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        insert_query_parser = INSERTQueryParser(query, self.get_columns())

        values_to_insert = insert_query_parser.parse_query()

        for value in values_to_insert:
            if not value.get("comment_id"):
                if not value.get("comment"):
                    raise ValueError("comment is mandatory for inserting a top-level comment.")
                else:
                    self.insert_comment(video_id=value["video_id"], text=value["comment"])

            else:
                if not value.get("reply"):
                    raise ValueError("reply is mandatory for inserting a reply.")
                else:
                    self.insert_comment(comment_id=value["comment_id"], text=value["reply"])

    def insert_comment(self, text, video_id: str = None, comment_id: str = None):
        # if comment_id is provided, define the request body for a reply and insert it
        if comment_id:
            request_body = {"snippet": {"parentId": comment_id, "textOriginal": text}}

            self.handler.connect().comments().insert(part="snippet", body=request_body).execute()

        # else if video_id is provided, define the request body for a top-level comment and insert it
        elif video_id:
            request_body = {"snippet": {"topLevelComment": {"snippet": {"videoId": video_id, "textOriginal": text}}}}

            self.handler.connect().commentThreads().insert(part="snippet", body=request_body).execute()

    def get_columns(self) -> List[str]:
        """Gets all columns to be returned in pandas DataFrame responses
        Returns
        -------
        List[str]
            List of columns
        """
        return [
            "comment_id",
            "channel_id",
            "video_id",
            "user_id",
            "display_name",
            "comment",
            "published_at",
            "updated_at",
            "reply_user_id",
            "reply_author",
            "reply",
        ]

    def get_comments(self, video_id: str, channel_id: str):
        """Pulls all the records from the given youtube api end point and returns it select()

        Returns
        -------
        pd.DataFrame of all the records of the "commentThreads()" API end point
        """

        if video_id and channel_id:
            channel_id = None

        resource = (
            self.handler.connect()
            .commentThreads()
            .list(
                part="snippet, replies",
                videoId=video_id,
                allThreadsRelatedToChannelId=channel_id,
                textFormat="plainText",
            )
        )

        data = []
        while resource:
            comments = resource.execute()

            for comment in comments["items"]:
                replies = []
                if "replies" in comment:
                    for reply in comment["replies"]["comments"]:
                        replies.append(
                            {
                                "reply_author": reply["snippet"]["authorDisplayName"],
                                "user_id": reply["snippet"]["authorChannelId"]["value"],
                                "reply": reply["snippet"]["textOriginal"],
                            }
                        )
                else:
                    replies.append(
                        {
                            "reply_author": None,
                            "user_id": None,
                            "reply": None,
                        }
                    )

                data.append(
                    {
                        "channel_id": comment["snippet"]["channelId"],
                        "video_id": comment["snippet"]["videoId"],
                        "user_id": comment["snippet"]["topLevelComment"]["snippet"]["authorChannelId"]["value"],
                        "comment_id": comment["snippet"]["topLevelComment"]["id"],
                        "display_name": comment["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                        "comment": comment["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                        "published_at": comment["snippet"]["topLevelComment"]["snippet"]["publishedAt"],
                        "updated_at": comment["snippet"]["topLevelComment"]["snippet"]["updatedAt"],
                        "replies": replies,
                    }
                )

            if "nextPageToken" in comments:
                resource = (
                    self.handler.connect()
                    .commentThreads()
                    .list(
                        part="snippet, replies",
                        videoId=video_id,
                        allThreadsRelatedToChannelId=channel_id,
                        textFormat="plainText",
                        pageToken=comments["nextPageToken"],
                    )
                )
            else:
                break

        youtube_comments_df = pd.json_normalize(
            data,
            "replies",
            [
                "comment_id",
                "channel_id",
                "video_id",
                "user_id",
                "display_name",
                "comment",
                "published_at",
                "updated_at",
            ],
            record_prefix="replies.",
        )
        youtube_comments_df = youtube_comments_df.rename(
            columns={
                "replies.user_id": "reply_user_id",
                "replies.reply_author": "reply_author",
                "replies.reply": "reply",
            }
        )

        # check if DataFrame is empty
        if youtube_comments_df.empty:
            return youtube_comments_df
        else:
            return youtube_comments_df[
                [
                    "comment_id",
                    "channel_id",
                    "video_id",
                    "user_id",
                    "display_name",
                    "comment",
                    "published_at",
                    "updated_at",
                    "reply_user_id",
                    "reply_author",
                    "reply",
                ]
            ]


class YoutubeChannelsTable(APITable):
    """Youtube Channel Info  by channel id Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        select_statement_parser = SELECTQueryParser(query, "channel", self.get_columns())

        (
            selected_columns,
            where_conditions,
            order_by_conditions,
            result_limit,
        ) = select_statement_parser.parse_query()

        channel_id = None
        for op, arg1, arg2 in where_conditions:
            if arg1 == "channel_id":
                if op == "=":
                    channel_id = arg2
                    break
                else:
                    raise NotImplementedError("Only '=' operator is supported for channel_id column.")

        if not channel_id:
            raise NotImplementedError("channel_id has to be present in where clause.")

        channel_df = self.get_channel_details(channel_id)

        select_statement_executor = SELECTQueryExecutor(
            channel_df,
            selected_columns,
            [where_condition for where_condition in where_conditions if where_condition[1] == "channel_id"],
            order_by_conditions,
            result_limit if query.limit else None,
        )

        channel_df = select_statement_executor.execute_query()

        return channel_df

    def get_channel_details(self, channel_id):
        details = (
            self.handler.connect().channels().list(part="statistics,snippet,contentDetails", id=channel_id).execute()
        )
        snippet = details["items"][0]["snippet"]
        statistics = details["items"][0]["statistics"]
        data = {
            "country": snippet["country"],
            "description": snippet["description"],
            "creation_date": snippet["publishedAt"],
            "title": snippet["title"],
            "subscriber_count": statistics["subscriberCount"],
            "video_count": statistics["videoCount"],
            "view_count": statistics["viewCount"],
            "channel_id": channel_id,
        }
        return pd.json_normalize(data)

    def get_columns(self) -> List[str]:
        return [
            "country",
            "description",
            "creation_date",
            "title",
            "subscriber_count",
            "video_count",
            "view_count",
            "channel_id",
        ]


class YoutubeVideosTable(APITable):
    """Youtube Video info  by video id Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        select_statement_parser = SELECTQueryParser(query, "video", self.get_columns())

        (
            selected_columns,
            where_conditions,
            order_by_conditions,
            result_limit,
        ) = select_statement_parser.parse_query()

        video_id, channel_id, search_query = None, None, None
        for op, arg1, arg2 in where_conditions:
            if arg1 == "video_id":
                if op == "=":
                    video_id = arg2
                else:
                    raise NotImplementedError("Only '=' operator is supported for video_id column.")

            elif arg1 == "channel_id":
                if op == "=":
                    channel_id = arg2
                else:
                    raise NotImplementedError("Only '=' operator is supported for channel_id column.")

            elif arg1 == "query":
                if op == "=":
                    search_query = arg2
                else:
                    raise NotImplementedError("Only '=' operator is supported for query column.")

        if not video_id and not channel_id and not search_query:
            raise ValueError("At least one of video_id, channel_id, or query must be present in the WHERE clause.")

        if video_id:
            video_df = self.get_videos_by_video_ids([video_id])
        elif channel_id and search_query:
            video_df = self.get_videos_by_search_query_in_channel(search_query, channel_id, result_limit)
        elif channel_id:
            video_df = self.get_videos_by_channel_id(channel_id, result_limit)
        else:
            video_df = self.get_videos_by_search_query(search_query, result_limit)

        select_statement_executor = SELECTQueryExecutor(
            video_df,
            selected_columns,
            [
                where_condition
                for where_condition in where_conditions
                if where_condition[1] not in ["video_id", "channel_id", "query"]
            ],
            order_by_conditions,
            result_limit if query.limit else None,
        )

        video_df = select_statement_executor.execute_query()

        return video_df

    def get_videos_by_search_query(self, search_query, limit=10):
        video_ids = []
        resource = (
            self.handler.connect()
            .search()
            .list(part="snippet", q=search_query, type="video", maxResults=min(50, limit))
        )
        total_fetched = 0

        while resource and total_fetched < limit:
            response = resource.execute()
            for item in response["items"]:
                video_ids.append(item["id"]["videoId"])
                total_fetched += 1
                if total_fetched >= limit:
                    break

            if "nextPageToken" in response and total_fetched < limit:
                resource = (
                    self.handler.connect()
                    .search()
                    .list(
                        part="snippet",
                        q=search_query,
                        type="video",
                        maxResults=min(50, limit - total_fetched),
                        pageToken=response["nextPageToken"],
                    )
                )
            else:
                break

        return self.get_videos_by_video_ids(video_ids)

    def get_videos_by_search_query_in_channel(self, search_query, channel_id, limit=10):
        """Search for videos within a specific channel"""
        video_ids = []
        resource = (
            self.handler.connect()
            .search()
            .list(part="snippet", q=search_query, channelId=channel_id, type="video", maxResults=min(50, limit))
        )
        total_fetched = 0

        while resource and total_fetched < limit:
            response = resource.execute()
            for item in response["items"]:
                video_ids.append(item["id"]["videoId"])
                total_fetched += 1
                if total_fetched >= limit:
                    break

            if "nextPageToken" in response and total_fetched < limit:
                resource = (
                    self.handler.connect()
                    .search()
                    .list(
                        part="snippet",
                        q=search_query,
                        channelId=channel_id,
                        type="video",
                        maxResults=min(50, limit - total_fetched),
                        pageToken=response["nextPageToken"],
                    )
                )
            else:
                break

        return self.get_videos_by_video_ids(video_ids)

    def get_videos_by_channel_id(self, channel_id, limit=10):
        video_ids = []
        resource = (
            self.handler.connect()
            .search()
            .list(part="snippet", channelId=channel_id, type="video", maxResults=min(50, limit))
        )
        total_fetched = 0
        while resource and total_fetched < limit:
            response = resource.execute()
            for item in response["items"]:
                video_ids.append(item["id"]["videoId"])
                total_fetched += 1
                if total_fetched >= limit:
                    break
            if "nextPageToken" in response and total_fetched < limit:
                resource = (
                    self.handler.connect()
                    .search()
                    .list(
                        part="snippet",
                        channelId=channel_id,
                        type="video",
                        maxResults=min(50, limit - total_fetched),
                        pageToken=response["nextPageToken"],
                    )
                )
            else:
                break

        return self.get_videos_by_video_ids(video_ids)

    def get_videos_by_video_ids(self, video_ids):
        data = []

        if not isinstance(video_ids, list):
            logger.error(f"video_ids must be a list. Received {type(video_ids)} instead.")
            return pd.DataFrame()

        # loop over 50 video ids at a time
        # an invalid request error is caused otherwise
        for i in range(0, len(video_ids), 50):
            resource = (
                self.handler.connect()
                .videos()
                .list(part="statistics,snippet,contentDetails", id=",".join(video_ids[i : i + 50]))
                .execute()
            )

            for item in resource["items"]:
                data.append(
                    {
                        "channel_id": item["snippet"]["channelId"],
                        "channel_title": item["snippet"]["channelTitle"],
                        "comment_count": item["statistics"]["commentCount"],
                        "description": item["snippet"]["description"],
                        "like_count": item["statistics"]["likeCount"],
                        "publish_time": item["snippet"]["publishedAt"],
                        "title": item["snippet"]["title"],
                        "transcript": self.get_captions_by_video_id(item["id"]),
                        "video_id": item["id"],
                        "view_count": item["statistics"]["viewCount"],
                        "duration_str": self.parse_duration(item["id"], item["contentDetails"]["duration"]),
                    }
                )

        return pd.json_normalize(data)

    def get_captions_by_video_id(self, video_id):
        try:
            transcript_response = YouTubeTranscriptApi.get_transcript(video_id, preserve_formatting=True)
            json_formatted_transcript = JSONFormatter().format_transcript(transcript_response, indent=2)
            return json_formatted_transcript

        except Exception as e:
            (logger.error(f"Encountered an error while fetching transcripts for video ${video_id}: ${e}"),)
            return "Transcript not available for this video"

    def parse_duration(self, video_id, duration):
        try:
            parsed_duration = re.search(r"PT(\d+H)?(\d+M)?(\d+S)", duration).groups()
            duration_str = ""
            for d in parsed_duration:
                if d:
                    duration_str += f"{d[:-1]}:"

            return duration_str.strip(":")
        except Exception as e:
            (logger.error(f"Encountered an error while parsing duration for video ${video_id}: ${e}"),)
            return "Duration not available for this video"

    def get_columns(self) -> List[str]:
        return [
            "channel_id",
            "channel_title",
            "title",
            "description",
            "publish_time",
            "comment_count",
            "like_count",
            "view_count",
            "video_id",
            "duration_str",
            "transcript",
        ]

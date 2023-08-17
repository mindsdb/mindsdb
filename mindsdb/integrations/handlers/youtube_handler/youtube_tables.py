from typing import List

from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from mindsdb.utilities import log

from mindsdb_sql.parser import ast


from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import os
import pandas as pd


logger = log.getLogger(__name__)

class YoutubeGetCommentsTable(APITable):
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
        conditions = extract_comparison_conditions(query.where)

        order_by_conditions = {}
        clubs_kwargs = {}

        if query.order_by and len(query.order_by) > 0:
            order_by_conditions["columns"] = []
            order_by_conditions["ascending"] = []

            for an_order in query.order_by:
                if an_order.field.parts[0] != "id":
                    next    
                if an_order.field.parts[1] in self.get_columns():
                    order_by_conditions["columns"].append(an_order.field.parts[1])

                    if an_order.direction == "ASC":
                        order_by_conditions["ascending"].append(True)
                    else:
                        order_by_conditions["ascending"].append(False)
                else:
                    raise ValueError(
                        f"Order by unknown column {an_order.field.parts[1]}"
                    )

        for a_where in conditions:
            if a_where[1] == "youtube_video_id":
                if a_where[0] != "=":
                    raise ValueError("Unsupported where operation for youtube video id")
                clubs_kwargs["type"] = a_where[2]
            else:
                raise ValueError(f"Unsupported where argument {a_where[1]}")


        youtube_comments_df = self.call_youtube_comments_api(a_where[2])

        selected_columns = []
        for target in query.targets:
            if isinstance(target, ast.Star):
                selected_columns = self.get_columns()
                break
            elif isinstance(target, ast.Identifier):
                selected_columns.append(target.parts[-1])
            else:
                raise ValueError(f"Unknown query target {type(target)}")


        if len(youtube_comments_df) == 0:
            youtube_comments_df = pd.DataFrame([], columns=selected_columns)
        else:
            youtube_comments_df.columns = self.get_columns()
            for col in set(youtube_comments_df.columns).difference(set(selected_columns)):
                youtube_comments_df = youtube_comments_df.drop(col, axis=1)

            if len(order_by_conditions.get("columns", [])) > 0:
                youtube_comments_df = youtube_comments_df.sort_values(
                    by=order_by_conditions["columns"],
                    ascending=order_by_conditions["ascending"],
                )

        if query.limit:
            youtube_comments_df = youtube_comments_df.head(query.limit.value)

        return youtube_comments_df

    def get_columns(self) -> List[str]:
        """Gets all columns to be returned in pandas DataFrame responses
        Returns
        -------
        List[str]
            List of columns
        """
        return [
        'user_id', 
        'display_name', 
        'comment'
        ]

    def call_youtube_comments_api(self,video_id):
        """Pulls all the records from the given youtube api end point and returns it select()
    
        Returns
        -------
        pd.DataFrame of all the records of the "commentThreads()" API end point
        """

        resource = self.handler.connect().commentThreads().list(part='snippet',videoId=video_id,textFormat='plainText')

        video_cols = self.get_columns()
        all_youtube_comments_df = pd.DataFrame(columns=video_cols)
        data = []

        while resource:
            comments = resource.execute()
            for comment in comments['items']:
                user_id = comment['snippet']['topLevelComment']['snippet']['authorChannelId']['value']
                display_name = comment['snippet']['topLevelComment']['snippet']['authorDisplayName']
                comment_text = comment['snippet']['topLevelComment']['snippet']['textDisplay']
                data = pd.DataFrame({
                    'user_id': [user_id],
                    'display_name': [display_name],
                    'comment': [comment_text]
                })
                all_youtube_comments_df = pd.concat([all_youtube_comments_df, data], ignore_index=True)
            if 'nextPageToken' in comments:
                resource = self.handler.connect().commentThreads().list(
                    part='snippet',
                    videoId=video_id,
                    textFormat='plainText',
                    pageToken=comments['nextPageToken']
                )
            else:
                break


        return all_youtube_comments_df

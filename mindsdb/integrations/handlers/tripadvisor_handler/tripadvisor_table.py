import pandas as pd
from mindsdb.integrations.libs.api_handler import APITable
from mindsdb_sql.parser import ast
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions


class SearchLocationTable(APITable):
    def select(self, query: ast.Select) -> pd.DataFrame:
        """Select data from the comment table and return it as a pandas DataFrame.

        Args:
            query (ast.Select): The SQL query to be executed.

        Returns:
            pandas.DataFrame: A pandas DataFrame containing the selected data.
        """

        tripAdvisor = self.handler.connect()

        submission_id = None
        conditions = extract_comparison_conditions(query.where)

        for condition in conditions:
            if condition[0] == "=" and condition[1] == "submission_id":
                submission_id = condition[2]
                break

        if submission_id is None:
            raise ValueError("Submission ID is missing in the SQL query")

        submission = reddit.submission(id=submission_id)
        submission.comments.replace_more(limit=None)

        result = []
        for comment in submission.comments.list():
            data = {
                "id": comment.id,
                "body": comment.body,
                "author": comment.author.name if comment.author else None,
                "created_utc": comment.created_utc,
                "score": comment.score,
                "permalink": comment.permalink,
                "ups": comment.ups,
                "downs": comment.downs,
                "subreddit": comment.subreddit.display_name,
            }
            result.append(data)

        result = pd.DataFrame(result)
        self.filter_columns(result, query)
        return result

    def get_columns(self):
        """Get the list of column names for the comment table.

        Returns:
            list: A list of column names for the comment table.
        """
        return [
            "location_id",
            "name",
            "distance",
            "rating",
            "bearing",
            "street1",
            "street2",
            "city",
            "state",
            "country",
            "postalcode",
            "address_string",
            "phone",
            "latitude",
            "longitude",
        ]

    def filter_columns(self, result: pd.DataFrame, query: ast.Select = None):
        columns = []
        if query is not None:
            for target in query.targets:
                if isinstance(target, ast.Star):
                    columns = self.get_columns()
                    break
                elif isinstance(target, ast.Identifier):
                    columns.append(target.value)
        if len(columns) > 0:
            result = result[columns]


class LocationDetailsTable(APITable):
    pass


class PhotosTable(APITable):
    pass

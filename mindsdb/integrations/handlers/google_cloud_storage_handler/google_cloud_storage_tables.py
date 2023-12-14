import pandas as pd
from mindsdb_sql.parser import ast

from mindsdb.integrations.handlers.utilities.query_utilities import INSERTQueryParser, UPDATEQueryParser, DELETEQueryParser
from mindsdb.integrations.libs.api_handler import APITable

from mindsdb.integrations.utilities.date_utils import parse_utc_date
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions


class GoogleCloudStorageBucketsTable(APITable):

    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Perform SELECT operation for Google Cloud Storage.

        Args:
            query (ast.Select): SQL query to parse.

        Returns:
            pd.DataFrame: Resulting data.
        """

        # Parse the query to get the conditions.
        conditions = extract_comparison_conditions(query.where)

        params = {}
        filters = []
        for op, arg1, arg2 in conditions:
            if arg1 in ["id", "name", "location", "storageClass"]:
                if op == "=":
                    params[arg1] = arg2
                else:
                    raise NotImplementedError(f"Not Supported Operator {op} for {arg1}")
            elif arg1 in ["timeCreated", "updated"]:
                if op == "=":
                    params[arg1] = parse_utc_date(arg2)
                elif op == ">":
                    params["after"] = parse_utc_date(arg2)
                elif op == "<":
                    params["before"] = parse_utc_date(arg2)
                else:
                    raise NotImplementedError(f"Not Supported Operator {op} for {arg1}")
            else:
                filters.append([op, arg1, arg2])

        # Get the order by from the query.
        if query.order_by is not None:
            if query.order_by[0].value == "timeCreated" or query.order_by[0].value == "updated":
                params["orderBy"] = query.order_by[0].value
            else:
                raise NotImplementedError(f"{query.order_by[0].value} is not supported")

        if query.limit is not None:
            params["limit"] = query.limit
        else:
            params["limit"] = 100

        select_cols = []
        if len(query.targets) > 0:
            for target in query.targets:
                if isinstance(target, ast.Star):
                    select_cols = self.get_columns()
                    break
                elif isinstance(target, ast.Identifier):
                    select_cols.append(target.parts[-1])
                else:
                    raise ValueError(f"Invalid query target {type(target)}")
        else:
            select_cols = self.get_columns()

        # Get list of buckets from the Google Cloud Storage API
        bucket_list = self.handler.call_application_api(
            method_name="list_buckets", params=params, filters=filters
        )

        if len(bucket_list) != 0:
            bucket_list.columns = self.get_columns()
            for col in set(bucket_list.columns).difference(set(select_cols)):
                bucket_list = bucket_list.drop(col, axis=1)
        else:
            bucket_list = pd.DataFrame([], columns=select_cols)

        return bucket_list

    def insert(self, query: ast.Insert) -> None:
        """
        Inserts a new bucket into the Storage.

        Args:
            query (ast.Insert): SQL query to parse.

        Returns:
            Response: Response object containing the results.
        """

        values = INSERTQueryParser(query).parse_query()

        bucket_data = {}
        target_columns = {
            "name",
            "user_project",
            "location",
            "storageClass"
        }

        for col, val in zip(query.columns, values):
            if col.name in target_columns:
                bucket_data[col.name] = val
            else:
                raise NotImplementedError(f"{col.name} with value {val} is not supported")

        self.handler.call_application_api(method_name="create_bucket", params=bucket_data)

    def update(self, query: ast.Update) -> None:
        """
        Updates a bucket in the Storage.

        Args:
            query (ast.Insert): SQL query to parse.

        Returns:
            Response: Response object containing the results.
        """

        values, conditions = UPDATEQueryParser(query).parse_query()

        bucket_data = {}
        target_columns = {
            "storageClass"
        }

        for op, arg1, arg2 in conditions:
            if arg1 == "name":
                if op == "=":
                    bucket_data[arg1] = arg2
                else:
                    raise NotImplementedError(f"Not Supported Operator {op} for {arg1}")

        for col, val in zip(query.update_columns, values):
            if col.name in target_columns:
                bucket_data[col.name] = val
            else:
                raise NotImplementedError(f"{col.name} with value {val} is not supported")

        self.handler.call_application_api(method_name="update_bucket", params=bucket_data)

    def delete(self, query: ast.Delete) -> None:
        """
        Deletes a bucket in the Storage.

        Args:
            query (ast.Insert): SQL query to parse.

        Returns:
            Response: Response object containing the results.
        """

        conditions = DELETEQueryParser(query).parse_query()

        bucket_data = {}
        for op, arg1, arg2 in conditions:
            if arg1 == "name":
                if op == "=":
                    bucket_data[arg1] = arg2
                else:
                    raise NotImplementedError(f"Not Supported Operator {op} for {arg1}")

        self.handler.call_application_api(method_name="delete_bucket", params=bucket_data)

    def get_columns(self):
        """
        Gets all columns to be returned in pandas DataFrame responses

        Returns:
            List[str]: List of columns
        """
        return [
            "selfLink",
            "id",
            "name",
            "projectNumber",
            "location",
            "storageClass",
            "timeCreated",
            "updated",
            "owner",
            "labels"
        ]

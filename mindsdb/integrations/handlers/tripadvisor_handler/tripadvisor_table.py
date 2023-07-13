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

        conditions = extract_comparison_conditions(query.where)

        params = {}
        filters = []
        for op, arg1, arg2 in conditions:
            if op == "or":
                raise NotImplementedError(f"OR is not supported")
            if arg1 == "searchQuery":
                if op == "=":
                    params[arg1] = arg2
                else:
                    NotImplementedError(f"Unknown op: {op}")
            elif arg1 == "category":
                if op == "=":
                    params[arg1] = arg2
                else:
                    NotImplementedError(f"Unknown op: {op}")
            elif arg1 == "phone":
                if op == "=":
                    params[arg1] = arg2
                else:
                    NotImplementedError(f"Unknown op: {op}")
            elif arg1 == "address":
                if op == "=":
                    params[arg1] = arg2
                else:
                    NotImplementedError(f"Unknown op: {op}")
            elif arg1 == "latLong":
                if op == "=":
                    params[arg1] = arg2
                else:
                    NotImplementedError(f"Unknown op: {op}")
            elif arg1 == "radius":
                if op == "=":
                    params[arg1] = arg2
                else:
                    NotImplementedError(f"Unknown op: {op}")
            elif arg1 == "radiusUnit":
                if op == "=":
                    params[arg1] = arg2
                else:
                    NotImplementedError(f"Unknown op: {op}")
            elif arg1 == "language":
                if op == "=":
                    params[arg1] = arg2
                else:
                    NotImplementedError(f"Unknown op: {op}")
            else:
                filters.append([op, arg1, arg2])

        if query.limit is not None:
            params["max_results"] = query.limit.value

        if "searchQuery" not in params:
            # search not works without searchQuery, use 'London'
            params["searchQuery"] = "London"

        result = self.handler.call_twitter_api(params=params, filters=filters)

        # filter targets
        columns = []
        for target in query.targets:
            if isinstance(target, ast.Star):
                columns = []
                break
            elif isinstance(target, ast.Identifier):
                columns.append(target.parts[-1])
            else:
                raise NotImplementedError

        if len(columns) == 0:
            columns = self.get_columns()

        # columns to lower case
        columns = [name.lower() for name in columns]

        if len(result) == 0:
            result = pd.DataFrame([], columns=columns)
        else:
            # add absent columns
            for col in set(columns) & set(result.columns) ^ set(columns):
                result[col] = None

            # filter by columns
            result = result[columns]
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

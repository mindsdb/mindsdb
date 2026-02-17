import pandas as pd
from mindsdb.integrations.libs.api_handler import APITable
from mindsdb_sql_parser import ast
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions


class SearchLocationTable(APITable):
    def select(self, query: ast.Select) -> pd.DataFrame:
        """Select data from the search_location table and return it as a pandas DataFrame.

        Args:
            query (ast.Select): The SQL query to be executed.

        Returns:
            pandas.DataFrame: A pandas DataFrame containing the selected data.
        """

        self.handler.connect()

        conditions = extract_comparison_conditions(query.where)

        allowed_keys = set(
            [
                "searchQuery",
                "category",
                "phone",
                "address",
                "latLong",
                "radius",
                "radiusUnit",
                "language",
            ]
        )

        params = {}
        filters = []
        for op, arg1, arg2 in conditions:
            if op == "or":
                raise NotImplementedError("OR is not supported")
            elif op == "=" and arg1 in allowed_keys:
                params[arg1] = arg2
            elif op != "=":
                raise NotImplementedError(f"Unknown op: {op}")
            else:
                filters.append([op, arg1, arg2])

        if query.limit is not None:
            params["max_results"] = query.limit.value

        if "searchQuery" not in params and "latLong" not in params:
            # search not works without searchQuery, use 'London'
            params["searchQuery"] = "London"

        result = self.handler.call_tripadvisor_searchlocation_api(params=params)

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
        """Get the list of column names for the search_location table.

        Returns:
            list: A list of column names for the search_location table.
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


class LocationDetailsTable(APITable):
    result_json = []

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Select data from the location_details table and return it as a pandas DataFrame.

        Args:
            query (ast.Select): The SQL query to be executed.

        Returns:
            pandas.DataFrame: A pandas DataFrame containing the selected data.
        """

        self.handler.connect()

        conditions = extract_comparison_conditions(query.where)

        allowed_keys = set(["locationId", "currency", "language"])

        params = {}
        filters = []
        for op, arg1, arg2 in conditions:
            if op == "or":
                raise NotImplementedError("OR is not supported")
            elif op == "=" and arg1 in allowed_keys:
                params[arg1] = arg2
            elif op != "=":
                raise NotImplementedError(f"Unknown op: {op}")
            else:
                filters.append([op, arg1, arg2])

        if query.limit is not None:
            params["max_results"] = query.limit.value

        if "locationId" not in params:
            # search not works without searchQuery, use 'London'
            params["locationId"] = "23322232"

        result = self.handler.call_tripadvisor_location_details_api(params=params)

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
        """Get the list of column names for the location_details table.

        Returns:
            list: A list of column names for the location_details table.
        """
        return [
            "location_id",
            "distance",
            "name",
            "description",
            "web_url",
            "street1",
            "street2",
            "city",
            "state",
            "country",
            "postalcode",
            "address_string",
            "latitude",
            "longitude",
            "timezone",
            "email",
            "phone",
            "website",
            "write_review",
            "ranking_data",
            "rating",
            "rating_image_url",
            "num_reviews",
            "photo_count",
            "see_all_photos",
            "price_level",
            "brand",
            "parent_brand",
            "ancestors",
            "periods",
            "weekday",
            "features",
            "cuisines",
            "amenities",
            "trip_types",
            "styles",
            "awards",
            "neighborhood_info",
            "parent_brand",
            "brand",
            "groups",
        ]


class ReviewsTable(APITable):
    def select(self, query: ast.Select) -> pd.DataFrame:
        """Select data from the reviews table and return it as a pandas DataFrame.

        Args:
            query (ast.Select): The SQL query to be executed.

        Returns:
            pandas.DataFrame: A pandas DataFrame containing the selected data.
        """

        self.handler.connect()

        conditions = extract_comparison_conditions(query.where)

        allowed_keys = set(["locationId", "language"])

        params = {}
        filters = []
        for op, arg1, arg2 in conditions:
            if op == "or":
                raise NotImplementedError("OR is not supported")
            elif op == "=" and arg1 in allowed_keys:
                params[arg1] = arg2
            elif op != "=":
                raise NotImplementedError(f"Unknown op: {op}")
            else:
                filters.append([op, arg1, arg2])

        if query.limit is not None:
            params["max_results"] = query.limit.value

        if "locationId" not in params:
            # search not works without searchQuery, use 'London'
            params["locationId"] = "23322232"

        result = self.handler.call_tripadvisor_reviews_api(params=params)

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
        """Get the list of column names for the reviews table.

        Returns:
            list: A list of column names for the reviews table.
        """
        return [
            "id",
            "lang",
            "location_id",
            "published_date",
            "rating",
            "helpful_votes",
            "rating_image_url",
            "url",
            "trip_type",
            "travel_date",
            "text_review",
            "title",
            "owner_response",
            "is_machine_translated",
            "user",
            "subratings",
        ]


class PhotosTable(APITable):
    def select(self, query: ast.Select) -> pd.DataFrame:
        """Select data from the photos table and return it as a pandas DataFrame.

        Args:
            query (ast.Select): The SQL query to be executed.

        Returns:
            pandas.DataFrame: A pandas DataFrame containing the selected data.
        """

        conditions = extract_comparison_conditions(query.where)

        allowed_keys = set(["locationId", "language"])

        params = {}
        filters = []
        for op, arg1, arg2 in conditions:
            if op == "or":
                raise NotImplementedError("OR is not supported")
            elif op == "=" and arg1 in allowed_keys:
                params[arg1] = arg2
            elif op != "=":
                raise NotImplementedError(f"Unknown op: {op}")
            else:
                filters.append([op, arg1, arg2])

        if query.limit is not None:
            params["max_results"] = query.limit.value

        if "locationId" not in params:
            params["locationId"] = "23322232"

        result = self.handler.call_tripadvisor_photos_api(params=params)

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
        """Get the list of column names for the photos table.

        Returns:
            list: A list of column names for the photos table.
        """
        return [
            "id",
            "is_blessed",
            "album",
            "caption",
            "published_date",
            "images",
            "source",
            "user",
        ]


class NearbyLocationTable(APITable):
    def select(self, query: ast.Select) -> pd.DataFrame:
        """Select data from the nearby_location table and return it as a pandas DataFrame.

        Args:
            query (ast.Select): The SQL query to be executed.

        Returns:
            pandas.DataFrame: A pandas DataFrame containing the selected data.
        """

        conditions = extract_comparison_conditions(query.where)

        allowed_keys = set(["latLong", "language", "category", "phone", "address", "radius", "radiusUnit"])

        params = {}
        filters = []
        for op, arg1, arg2 in conditions:
            if op == "or":
                raise NotImplementedError("OR is not supported")
            elif op == "=" and arg1 in allowed_keys:
                params[arg1] = arg2
            elif op != "=":
                raise NotImplementedError(f"Unknown op: {op}")
            else:
                filters.append([op, arg1, arg2])

        if query.limit is not None:
            params["max_results"] = query.limit.value

        if "latLong" not in params:
            params["latLong"] = "40.780825, -73.972781"

        result = self.handler.call_tripadvisor_nearby_location_api(params=params)

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
        """Get the list of column names for the nearby_location table.

        Returns:
            list: A list of column names for the nearby_location table.
        """
        return [
            "location_id",
            "name",
            "distance",
            "rating",
            "bearing",
            "address_obj",
        ]

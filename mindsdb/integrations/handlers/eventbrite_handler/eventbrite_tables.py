import pandas as pd
import collections
from mindsdb.integrations.libs.api_handler import APITable
from mindsdb_sql_parser import ast
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from mindsdb.integrations.utilities.handlers.query_utilities import (
    SELECTQueryParser,
    SELECTQueryExecutor,
)


def flatten(d, parent_key="", sep="_"):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


class EventbriteUserTable(APITable):
    def select(self, query: ast.Select) -> pd.DataFrame:
        self.handler.connect()

        if query.limit is not None:
            raise NotImplementedError("Limit is not supported for user info table")

        user_info = self.handler.api.get_current_user()

        # Normalize email field
        if "emails" in user_info and isinstance(user_info["emails"], list):
            user_info["email"] = (
                user_info["emails"][0]["email"] if user_info["emails"] else None
            )
            user_info["email_verified"] = (
                user_info["emails"][0]["verified"] if user_info["emails"] else None
            )
            user_info["email_primary"] = (
                user_info["emails"][0]["primary"] if user_info["emails"] else None
            )
            del user_info["emails"]
        else:
            user_info["email"] = None
            user_info["email_verified"] = None
            user_info["email_primary"] = None

        data = pd.DataFrame([user_info])

        # Select columns based on query
        columns = self.get_columns()
        selected_columns = [
            target.parts[-1]
            for target in query.targets
            if isinstance(target, ast.Identifier)
        ]
        if selected_columns:
            columns = [col for col in columns if col in selected_columns]
        data = data[columns]

        return data

    def get_columns(self):
        return [
            "email",
            "email_verified",
            "email_primary",
            "id",
            "name",
            "first_name",
            "last_name",
            "is_public",
            "image_id",
        ]


class EventbriteOrganizationTable(APITable):
    def select(self, query: ast.Select) -> pd.DataFrame:
        self.handler.connect()

        organization_info = self.handler.api.get_user_organizations()

        # Normalize organization data
        organizations = organization_info.get("organizations", [])
        result = pd.DataFrame(organizations)

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
        return [
            "_type",
            "name",
            "vertical",
            "parent_id",
            "locale",
            "created",
            "image_id",
            "id",
        ]


class EventbriteCategoryTable(APITable):
    def select(self, query: ast.Select) -> pd.DataFrame:
        self.handler.connect()

        category_info = self.handler.api.list_categories()
        categories = category_info.get("categories", [])
        result = pd.DataFrame(categories)

        select_statement_parser = SELECTQueryParser(
            query, "categoryInfoTable", self.get_columns()
        )

        (
            selected_columns,
            where_conditions,
            order_by_conditions,
            result_limit,
        ) = select_statement_parser.parse_query()

        total_results = result_limit if result_limit else 100

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

        select_statement_executor = SELECTQueryExecutor(
            result, selected_columns, where_conditions, order_by_conditions
        )

        result = select_statement_executor.execute_query()

        return result.head(total_results)

    def get_columns(self):
        return [
            "resource_uri",
            "id",
            "name",
            "name_localized",
            "short_name",
            "short_name_localized",
        ]


class EventbriteSubcategoryTable(APITable):
    def select(self, query: ast.Select) -> pd.DataFrame:
        self.handler.connect()

        # Dummy function to simulate API call and response
        category_info = self.handler.api.list_subcategories()

        # Normalizing the category data
        categories = category_info.get("subcategories", [])
        result = pd.DataFrame(categories)

        select_statement_parser = SELECTQueryParser(
            query, "subcategoryInfoTable", self.get_columns()
        )

        (
            selected_columns,
            where_conditions,
            order_by_conditions,
            result_limit,
        ) = select_statement_parser.parse_query()

        total_results = result_limit if result_limit else 100

        # Normalize nested fields
        parent_category = result["parent_category"].apply(pd.Series)
        parent_category.columns = [f"parent_{col}" for col in parent_category.columns]
        result = pd.concat([result, parent_category], axis=1).drop(
            "parent_category", axis=1
        )

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

        select_statement_executor = SELECTQueryExecutor(
            result, selected_columns, where_conditions, order_by_conditions
        )

        result = select_statement_executor.execute_query()

        return result.head(total_results)

    def get_columns(self):
        return [
            "resource_uri",
            "id",
            "name",
            "name_localized",
            "parent_category",
            "parent_resource_uri",
            "parent_id",
            "parent_name",
            "parent_name_localized",
            "parent_short_name",
            "parent_short_name_localized",
        ]


class EventbriteFormatTable(APITable):
    def select(self, query: ast.Select) -> pd.DataFrame:
        self.handler.connect()

        # Simulate API call and get the response
        format_info = self.handler.api.list_formats()

        # Normalize format data
        formats = format_info.get("formats", [])
        result = pd.DataFrame(formats)

        select_statement_parser = SELECTQueryParser(
            query, "formatInfoTable", self.get_columns()
        )

        (
            selected_columns,
            where_conditions,
            order_by_conditions,
            result_limit,
        ) = select_statement_parser.parse_query()

        total_results = result_limit if result_limit else 100

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

        select_statement_executor = SELECTQueryExecutor(
            result, selected_columns, where_conditions, order_by_conditions
        )

        result = select_statement_executor.execute_query()

        return result.head(total_results)

    def get_columns(self):
        return [
            "resource_uri",
            "id",
            "name",
            "name_localized",
            "short_name",
            "short_name_localized",
        ]


class EventbriteEventDetailsTable(APITable):
    def select(self, query: ast.Select) -> pd.DataFrame:
        self.handler.connect()

        conditions = extract_comparison_conditions(query.where)
        allowed_keys = set(["event_id"])

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

        if "event_id" not in params:
            # search not works without searchQuery, use 'London'
            params["event_id"] = "717926867587"

        event_details = self.handler.api.get_event(params["event_id"])

        # Normalize event data
        flat_event_details = flatten(event_details)
        result = pd.DataFrame([flat_event_details])

        for col in ["name", "description", "start", "end", "logo"]:
            if col in result.columns:
                result = pd.concat(
                    [result, result[col].apply(pd.Series).add_prefix(f"{col}_")],
                    axis=1,
                )
                result = result.drop([col], axis=1)

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
        return [
            "name_text",
            "name_html",
            "description_text",
            "description_html",
            "url",
            "start_timezone",
            "start_local",
            "start_utc",
            "end_timezone",
            "end_local",
            "end_utc",
            "organization_id",
            "created",
            "changed",
            "published",
            "capacity",
            "capacity_is_custom",
            "status",
            "currency",
            "listed",
            "shareable",
            "online_event",
            "tx_time_limit",
            "hide_start_date",
            "hide_end_date",
            "locale",
            "is_locked",
            "privacy_setting",
            "is_series",
            "is_series_parent",
            "inventory_type",
            "is_reserved_seating",
            "show_pick_a_seat",
            "show_seatmap_thumbnail",
            "show_colors_in_seatmap_thumbnail",
            "source",
            "is_free",
            "version",
            "summary",
            "facebook_event_id",
            "logo_id",
            "organizer_id",
            "venue_id",
            "category_id",
            "subcategory_id",
            "format_id",
            "id",
            "resource_uri",
            "is_externally_ticketed",
            "logo_crop_mask",
            "logo_original",
            "logo_id",
            "logo_url",
            "logo_aspect_ratio",
            "logo_edge_color",
            "logo_edge_color_set",
        ]


class EventbriteEventsTable(APITable):
    def __init__(self, handler):
        self.handler = handler

    def select(self, query: ast.Select) -> pd.DataFrame:
        self.handler.connect()

        conditions = extract_comparison_conditions(query.where)
        allowed_keys = set(["organization_id"])

        params = {}
        for op, arg1, arg2 in conditions:
            if op == "=" and arg1 in allowed_keys:
                params[arg1] = arg2
            else:
                raise NotImplementedError(
                    f"Unsupported operation or field: {op} {arg1}"
                )

        if "organization_id" not in params:
            raise ValueError("Organization ID must be provided")

        event_list = self.handler.api.list_events(params["organization_id"])
        result = pd.DataFrame(event_list["events"])

        # Normalize event data and split nested dictionaries into separate columns
        result = pd.concat(
            [
                result.drop(["name", "description", "start", "end", "logo"], axis=1),
                result["name"].apply(pd.Series).add_prefix("name_"),
                result["description"].apply(pd.Series).add_prefix("description_"),
                result["start"].apply(pd.Series).add_prefix("start_"),
                result["end"].apply(pd.Series).add_prefix("end_"),
                result["logo"].apply(pd.Series).add_prefix("logo_"),
            ],
            axis=1,
        )

        select_statement_parser = SELECTQueryParser(
            query, "subcategoryInfoTable", self.get_columns()
        )

        (
            selected_columns,
            where_conditions,
            order_by_conditions,
            result_limit,
        ) = select_statement_parser.parse_query()

        total_results = result_limit if result_limit else 100

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

        select_statement_executor = SELECTQueryExecutor(
            result, selected_columns, where_conditions, order_by_conditions
        )

        result = select_statement_executor.execute_query()

        return result.head(total_results)

    def get_columns(self):
        return [
            "name_text",
            "name_html",
            "description_text",
            "description_html",
            "url",
            "start_timezone",
            "start_local",
            "start_utc",
            "end_timezone",
            "end_local",
            "end_utc",
            "organization_id",
            "created",
            "changed",
            "published",
            "capacity",
            "capacity_is_custom",
            "status",
            "currency",
            "listed",
            "shareable",
            "online_event",
            "tx_time_limit",
            "hide_start_date",
            "hide_end_date",
            "locale",
            "is_locked",
            "privacy_setting",
            "is_series",
            "is_series_parent",
            "inventory_type",
            "is_reserved_seating",
            "show_pick_a_seat",
            "show_seatmap_thumbnail",
            "show_colors_in_seatmap_thumbnail",
            "source",
            "is_free",
            "summary",
            "organizer_id",
            "venue_id",
            "category_id",
            "subcategory_id",
            "format_id",
            "id",
            "resource_uri",
            "is_externally_ticketed",
        ]

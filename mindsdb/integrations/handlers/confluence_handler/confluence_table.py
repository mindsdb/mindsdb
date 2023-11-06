from typing import List

import pandas as pd
from mindsdb_sql.parser import ast

from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from mindsdb.utilities.log import get_log

logger = get_log("integrations.confluence_handler")


class ConfluenceSpacesTable(APITable):
    """Confluence Spaces Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Confluence "get_all_spaces" API endpoint
        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query
        Returns
        -------
        pd.DataFrame
            confluence "get_all_spaces" matching the query
        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """
        conditions = extract_comparison_conditions(query.where)

        if query.limit:
            total_results = query.limit.value
        else:
            total_results = 50

        spaces_kwargs = {}
        order_by_conditions = {}

        if query.order_by and len(query.order_by) > 0:
            order_by_conditions["columns"] = []
            order_by_conditions["ascending"] = []

            for an_order in query.order_by:
                if an_order.field.parts[0] != "":
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
            if a_where[1] == "type":
                if a_where[0] != "=":
                    raise ValueError("Unsupported where operation for type")
                if a_where[2] not in ["personal", "global"]:
                    raise ValueError(
                        f"Unsupported where argument for state {a_where[2]}"
                    )
                spaces_kwargs["type"] = a_where[2]
            else:
                raise ValueError(f"Unsupported where argument {a_where[1]}")

        confluence_spaces_records = self.handler.connect().get_all_spaces(
            start=0, limit=total_results
        )
        confluence_spaces_df = pd.json_normalize(confluence_spaces_records["results"])
        confluence_spaces_df = confluence_spaces_df[self.get_columns()]

        if "type" in spaces_kwargs:
            confluence_spaces_df = confluence_spaces_df[
                confluence_spaces_df.type == spaces_kwargs["type"]
            ]

        selected_columns = []
        for target in query.targets:
            if isinstance(target, ast.Star):
                selected_columns = self.get_columns()
                break
            elif isinstance(target, ast.Identifier):
                selected_columns.append(target.parts[-1])
            else:
                raise ValueError(f"Unknown query target {type(target)}")

        if len(confluence_spaces_df) == 0:
            confluence_spaces_df = pd.DataFrame([], columns=selected_columns)
        else:
            confluence_spaces_df.columns = self.get_columns()
            for col in set(confluence_spaces_df.columns).difference(
                set(selected_columns)
            ):
                confluence_spaces_df = confluence_spaces_df.drop(col, axis=1)

            if len(order_by_conditions.get("columns", [])) > 0:
                confluence_spaces_df = confluence_spaces_df.sort_values(
                    by=order_by_conditions["columns"],
                    ascending=order_by_conditions["ascending"],
                )

        return confluence_spaces_df

    def get_columns(self) -> List[str]:
        """Gets all columns to be returned in pandas DataFrame responses
        Returns
        -------
        List[str]
            List of columns
        """
        return [
            "id",
            "key",
            "name",
            "type",
            "_links.self",
            "_links.webui",
        ]


class ConfluencePagesTable(APITable):
    """Confluence Pages Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Confluence "get_all_pages_from_space" API endpoint
        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query
        Returns
        -------
        pd.DataFrame
            confluence "get_all_pages_from_space" matching the query
        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """
        conditions = extract_comparison_conditions(query.where)

        if query.limit:
            total_results = query.limit.value
        else:
            total_results = 50

        pages_kwargs = {}
        order_by_conditions = {}

        if query.order_by and len(query.order_by) > 0:
            order_by_conditions["columns"] = []
            order_by_conditions["ascending"] = []

            for an_order in query.order_by:
                if an_order.field.parts[0] != "":
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
            if a_where[1] == "space":
                if a_where[0] != "=":
                    raise ValueError("Unsupported where operation for space")
                pages_kwargs["space"] = a_where[2]
            else:
                raise ValueError(f"Unsupported where argument {a_where[1]}")

        confluence_pages_records = self.handler.connect().get_all_pages_from_space(
            a_where[2], start=0, limit=total_results, expand="body.storage"
        )
        confluence_pages_df = pd.json_normalize(confluence_pages_records)
        confluence_pages_df = confluence_pages_df[self.get_columns()]

        def extract_space(input_string):
            parts = input_string.split('/')
            return parts[-1]

        confluence_pages_df["space"] = confluence_pages_df["_expandable.space"].apply(
            extract_space
        )

        if "space" in pages_kwargs:
            confluence_pages_df = confluence_pages_df[
                confluence_pages_df.space == pages_kwargs["space"]
            ]

        selected_columns = []
        for target in query.targets:
            if isinstance(target, ast.Star):
                selected_columns = self.get_columns()
                break
            elif isinstance(target, ast.Identifier):
                selected_columns.append(target.parts[-1])
            else:
                raise ValueError(f"Unknown query target {type(target)}")

        if len(confluence_pages_df) == 0:
            confluence_pages_df = pd.DataFrame([], columns=selected_columns)
        else:
            confluence_pages_df.columns = self.get_columns()
            for col in set(confluence_pages_df.columns).difference(
                set(selected_columns)
            ):
                confluence_pages_df = confluence_pages_df.drop(col, axis=1)

            if len(order_by_conditions.get("columns", [])) > 0:
                confluence_pages_df = confluence_pages_df.sort_values(
                    by=order_by_conditions["columns"],
                    ascending=order_by_conditions["ascending"],
                )

        return confluence_pages_df

    def get_columns(self) -> List[str]:
        """Gets all columns to be returned in pandas DataFrame responses
        Returns
        -------
        List[str]
            List of columns
        """
        return [
            "id",
            "type",
            "status",
            "title",
            "body.storage.value",
            "_expandable.space",
            "_links.self",
            "_links.webui",
        ]

    def insert(self, query: ast.Insert):
        """Inserts a new page into the Confluence space

        Parameters
        ----------
        query : ast.Insert
            Given SQL INSERT query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        new_page = {}
        for i, column in enumerate(self.get_columns()):
            new_page[column] = query.values[0][i]

        self.handler.connect().create_page(
            space=new_page["space"],
            title=new_page["title"],
            body=new_page["body.storage.value"],
        )

    def update(self, query: ast.Update):
        """Updates a page in the Confluence space

        Parameters
        ----------
        query : ast.Update
            Given SQL UPDATE query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        update_page = {}
        for i, column in enumerate(self.get_columns()):
            update_page[column] = query.values[0][i]

        self.handler.connect().update_page(
            page_id=update_page["id"],
            title=update_page["title"],
            body=update_page["body.storage.value"],
        )

    def delete(self, query: ast.Delete):
        """Deletes a page from the Confluence space

        Parameters
        ----------
        query : ast.Delete
            Given SQL DELETE query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        delete_page = {}
        for i, column in enumerate(self.get_columns()):
            delete_page[column] = query.values[0][i]

        self.handler.connect().remove_page(
            page_id=delete_page["id"],
        )

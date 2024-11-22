from typing import List

import pandas as pd
from mindsdb_sql_parser import ast

from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from mindsdb.utilities import log

from mindsdb_sql_parser import ast

logger = log.getLogger(__name__)


class ConfluencePagesTable(APITable):
    """Confluence Pages Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the Confluence "get_all_pages_from_space" API endpoint"""
        conditions = extract_comparison_conditions(query.where)
        if query.limit:
            total_results = query.limit.value
        else:
            total_results = 50

        space_name = None
        page_id = None

        for a_where in conditions:
            if a_where[1] == "space":
                space_name = a_where[2]
                if a_where[0] != "=":
                    raise ValueError("Unsupported where operation for space")
            elif a_where[1] == "id":
                page_id = a_where[2]
                if a_where[0] != "=":
                    raise ValueError("Unsupported where operation for id")
            elif a_where[1] not in ["id", "space"]:
                raise ValueError(f"Unsupported where argument {a_where[1]}")

        if space_name is None:
            raise ValueError("Space name must be provided in the WHERE clause")

        if page_id is not None:
            try:
                page = self.handler.connect().get_page_by_id(page_id, expand='body.storage,space')
                confluence_pages_records = [page] if page['space']['key'] == space_name else []
            except Exception as e:
                logger.error(f"Error fetching page with ID {page_id}: {str(e)}")
                confluence_pages_records = []
        else:
            confluence_pages_records = self.handler.connect().get_all_pages_from_space(
                space_name, start=0, limit=total_results, expand="body.storage,space"
            )

        if not confluence_pages_records:
            return pd.DataFrame(columns=self.get_columns())

        confluence_pages_df = pd.json_normalize(confluence_pages_records)
        
        confluence_pages_df.columns = confluence_pages_df.columns.str.replace("body.storage.value", "body")
        confluence_pages_df['space'] = confluence_pages_df['space.key']
        
        confluence_pages_df = confluence_pages_df[self.get_columns()]

        selected_columns = []
        for target in query.targets:
            if isinstance(target, ast.Star):
                selected_columns = self.get_columns()
                break
            elif isinstance(target, ast.Identifier):
                col = target.parts[-1]
                if col in self.get_columns():
                    selected_columns.append(col)
                else:
                    raise ValueError(f"Unknown column: {col}")
            else:
                raise ValueError(f"Unknown query target {type(target)}")

        confluence_pages_df = confluence_pages_df[selected_columns]

        if query.order_by and len(query.order_by) > 0:
            sort_columns = []
            sort_ascending = []
            for an_order in query.order_by:
                if isinstance(an_order.field, ast.Identifier):
                    column = an_order.field.parts[-1]
                    if column in selected_columns:
                        sort_columns.append(column)
                        sort_ascending.append(an_order.direction == "ASC")
                    else:
                        raise ValueError(f"Order by unknown column {column}")
                else:
                    raise ValueError(f"Unsupported order by clause: {an_order}")
            
            if sort_columns:
                confluence_pages_df = confluence_pages_df.sort_values(
                    by=sort_columns,
                    ascending=sort_ascending
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
            "space",
            "body",
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

        columns = [col.name for col in query.columns]

        for i, val in enumerate(query.values):
            params = dict(zip(columns, val))

            self.handler.connect().create_page(
                space=params["space"],
                title=params["title"],
                body=params["body"],
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
        params = extract_comparison_conditions(query.where)
        title = query.update_columns["title"].get_string()
        body = query.update_columns["body"].get_string()

        for param in params:
            if param[1] == "id":
                id = param[2]
                self.handler.connect().update_page(
                    page_id=id,
                    title=title,
                    body=body,
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
        params = extract_comparison_conditions(query.where)
        for param in params:
            if param[1] == "id":
                id = param[2]
                self.handler.connect().remove_page(
                    page_id=id,
                )

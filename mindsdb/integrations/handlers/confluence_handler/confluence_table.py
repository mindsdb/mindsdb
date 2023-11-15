import pandas as pd

from typing import List

from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from mindsdb.utilities import log

from mindsdb_sql.parser import ast

logger = log.getLogger(__name__)

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

        confluence_spaces_records = self.handler.connect().get_all_spaces(start=0,limit=total_results)
        confluence_spaces_df = pd.json_normalize(confluence_spaces_records["results"])
        confluence_spaces_df = confluence_spaces_df[self.get_columns()]

        if "type" in spaces_kwargs:
            confluence_spaces_df = confluence_spaces_df[confluence_spaces_df.type == spaces_kwargs["type"]]
        
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
            for col in set(confluence_spaces_df.columns).difference(set(selected_columns)):
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

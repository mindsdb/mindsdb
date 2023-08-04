from typing import List

from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from mindsdb.utilities.log import get_log

from mindsdb_sql.parser import ast


from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import os
import pandas as pd


logger = get_log("integrations.google_docs_handler")

class GoogleDocGetDetailsTable(APITable):
    """Google Doc details  by doc id Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the google doc  "GET /v1/documents/{documentId}" API endpoint
        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query
        Returns
        -------
        pd.DataFrame
           google doc  "GET /v1/documents/{documentId}" matching the query
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
            if a_where[1] == "google_doc_id":
                if a_where[0] != "=":
                    raise ValueError("Unsupported where operation for google doc id")
                clubs_kwargs["type"] = a_where[2]
            else:
                raise ValueError(f"Unsupported where argument {a_where[1]}")


        googledoc_details_df = self.call_googledoc_details_api(a_where[2])

        selected_columns = []
        for target in query.targets:
            if isinstance(target, ast.Star):
                selected_columns = self.get_columns()
                break
            elif isinstance(target, ast.Identifier):
                selected_columns.append(target.parts[-1])
            else:
                raise ValueError(f"Unknown query target {type(target)}")


        if len(googledoc_details_df) == 0:
            googledoc_details_df = pd.DataFrame([], columns=selected_columns)
        else:
            googledoc_details_df.columns = self.get_columns()
            for col in set(googledoc_details_df.columns).difference(set(selected_columns)):
                googledoc_details_df = googledoc_details_df.drop(col, axis=1)

            if len(order_by_conditions.get("columns", [])) > 0:
                googledoc_details_df = googledoc_details_df.sort_values(
                    by=order_by_conditions["columns"],
                    ascending=order_by_conditions["ascending"],
                )

        if query.limit: 
            googledoc_details_df = googledoc_details_df.head(query.limit.value)

    
        return googledoc_details_df

    def get_columns(self) -> List[str]:
        """Gets all columns to be returned in pandas DataFrame responses
        Returns
        -------
        List[str]
            List of columns
        """
        return [
        'doc_title', 
        'doc_content'
        ]

    def call_googledoc_details_api(self,doc_id):
        """Pulls the title and contents from the given google doc and returns it select()
    
        Returns
        -------
        pd.DataFrame of title and contents from the given google doc id
        """

        document = self.handler.connect().documents().get(documentId=doc_id).execute()
        logger.error(f"document: {document}!")
        doc_cols = self.get_columns()
        all_googledoc_details_df = pd.DataFrame(columns=doc_cols)
        title = document.get('title')
        content = document.get('body').get('content')
        text = ""
        for element in content:
            if 'paragraph' in element:
                paragraph = element['paragraph']
                for run in paragraph['elements']:
                    text += run['textRun']['content']

        data = pd.DataFrame({'doc_title':[title],'doc_content':[text]})
        logger.error(f"data: {data}!")
        all_googledoc_details_df = pd.concat([all_googledoc_details_df, data], ignore_index=True)
        

        return all_googledoc_details_df

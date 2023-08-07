from typing import List

from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from mindsdb.utilities.log import get_log

from mindsdb_sql.parser import ast


from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

import os
import io
import pandas as pd


logger = get_log("integrations.google_drive_handler")

class GoogleDriveListPDFFilesTable(APITable):
    """Google Drive list pdf files implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the google drive API endpoint
        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query
        Returns
        -------
        pd.DataFrame matching the query
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

        googledrive_list_pdf_df = self.call_googledrive_list_files_api()

        selected_columns = []
        for target in query.targets:
            if isinstance(target, ast.Star):
                selected_columns = self.get_columns()
                break
            elif isinstance(target, ast.Identifier):
                selected_columns.append(target.parts[-1])
            else:
                raise ValueError(f"Unknown query target {type(target)}")


        if len(googledrive_list_pdf_df) == 0:
            googledrive_list_pdf_df = pd.DataFrame([], columns=selected_columns)
        else:
            googledrive_list_pdf_df.columns = self.get_columns()
            for col in set(googledrive_list_pdf_df.columns).difference(set(selected_columns)):
                googledrive_list_pdf_df = googledrive_list_pdf_df.drop(col, axis=1)

            if len(order_by_conditions.get("columns", [])) > 0:
                googledrive_list_pdf_df = googledrive_list_pdf_df.sort_values(
                    by=order_by_conditions["columns"],
                    ascending=order_by_conditions["ascending"],
                )

        if query.limit: 
            googledrive_list_pdf_df = googledrive_list_pdf_df.head(query.limit.value)

    
        return googledrive_list_pdf_df

    def get_columns(self) -> List[str]:
        """Gets all columns to be returned in pandas DataFrame responses
        Returns
        -------
        List[str]
            List of columns
        """
        return [
        'id', 
        'name'
        ]

    def call_googledrive_list_files_api(self):
        """Pulls the title and contents from the given google doc and returns it select()
    
        Returns
        -------
        pd.DataFrame of title and contents from the given google doc id
        """

        pdflist = self.handler.connect().files().list(q="mimeType='application/pdf'",pageSize=100, fields="nextPageToken, files(id, name)").execute()
        items = pdflist.get('files', [])
        logger.error(f"pdflist: {items}!")
        drive_cols = self.get_columns()
        df = pd.json_normalize(items)
        df = df[drive_cols]
       
        logger.error(f"data: {df}!")

        return df


class GoogleDriveDownloadFileTable(APITable):
    """Google Drive pdf file download implementation"""
  
    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the google drive API endpoint
        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query
        Returns
        -------
        pd.DataFrame matching the query
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
            if a_where[1] == "google_drive_file_id":
                if a_where[0] != "=":
                    raise ValueError("Unsupported where operation for google doc id")
                clubs_kwargs["type"] = a_where[2]
            else:
                    raise ValueError(f"Unsupported where argument {a_where[1]}")

        googledrive_pdf_df = self.call_googledrive_download_files_api(a_where[2])

        return googledrive_pdf_df

    def call_googledrive_download_files_api(self,file_id):
        """Pulls the title and contents from the given google doc and returns it select()
    
        Returns
        -------
        pd.DataFrame of title and contents from the given google doc id
        """

        pdf_dl_request = self.handler.connect().files().get_media(fileId=file_id)
        file_metadata = self.handler.connect().files().get(fileId=file_id, fields='name').execute()

        file_name = file_metadata['name']
        file = io.BytesIO()
        downloader = MediaIoBaseDownload(file, pdf_dl_request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
        with open(file_name, 'wb') as f:
            f.write(file.getvalue())

        data = pd.DataFrame({'file_name':[file_name],'download_status':[done],'file_id':[file_id]})

        return data

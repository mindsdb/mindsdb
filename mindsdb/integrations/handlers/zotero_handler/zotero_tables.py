import pandas as pd
from mindsdb.utilities import log
from mindsdb_sql_parser import ast
from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions

logger = log.getLogger(__name__)


class AnnotationsTable(APITable):
    """Represents a table of annotations in Zotero."""

    def select(self, query: ast.Select):
        """Select annotations based on the provided query.

        Parameters
        ----------
        query : ast.Select
            AST (Abstract Syntax Tree) representation of the SQL query.

        Returns
        -------
        Response
            Response object containing the selected annotations as a DataFrame.
        """
        if query.where is None:  # Handle case for SELECT * FROM annotations
            df = self._get_items()
            return df[self.get_columns()]

        conditions = extract_comparison_conditions(query.where)
        supported = False  # Flag to check if the query is supported

        for op, arg1, arg2 in conditions:
            if op in {'or', 'and'}:
                raise NotImplementedError('OR and AND are not supported')
            if arg1 == 'item_id' and op == '=':
                df = self._get_item(arg2)
                supported = True
            elif arg1 == 'parent_item_id' and op == '=':
                df = self._get_item_children(arg2)
                supported = True

        if not supported:
            raise NotImplementedError('Only "item_id=" and "parent_item_id=" conditions are implemented')

        return df[self.get_columns()]

    def get_columns(self):
        """Get the columns of the annotations table.

        Returns
        -------
        list
            List of column names.
        """
        return [
            'annotationColor',
            'annotationComment',
            'annotationPageLabel',
            'annotationText',
            'annotationType',
            'dateAdded',
            'dateModified',
            'key',
            'parentItem',
            'relations',
            'tags',
            'version'
        ]

    def _get_items(self) -> pd.DataFrame:
        """Get all annotations from the Zotero API.

        Returns
        -------
        pd.DataFrame
            DataFrame containing all annotations.
        """
        if not self.handler.is_connected:
            self.handler.connect()

        try:
            method = getattr(self.handler.api, 'items')
            result = method(itemType='annotation')

            if isinstance(result, dict):
                return pd.DataFrame([result.get('data', {})])
            if isinstance(result, list) and all(isinstance(item, dict) for item in result):
                data_list = [item.get('data', {}) for item in result]
                return pd.DataFrame(data_list)

        except Exception as e:
            logger.error(f"Error fetching items: {e}")
            raise e

        return pd.DataFrame()

    def _get_item(self, item_id: str) -> pd.DataFrame:
        """Get a single annotation by item ID.

        Parameters
        ----------
        item_id : str
            The ID of the item to fetch.

        Returns
        -------
        pd.DataFrame
            DataFrame containing the annotation.
        """
        if not self.handler.is_connected:
            self.handler.connect()

        try:
            method = getattr(self.handler.api, 'item')
            result = method(item_id, itemType='annotation')

            if isinstance(result, dict):
                return pd.DataFrame([result.get('data', {})])

        except Exception as e:
            logger.error(f"Error fetching item with ID {item_id}: {e}")
            raise e

        return pd.DataFrame()

    def _get_item_children(self, parent_item_id: str) -> pd.DataFrame:
        """Get annotations for a specific parent item ID.

        Parameters
        ----------
        parent_item_id : str
            The parent item ID to fetch annotations for.

        Returns
        -------
        pd.DataFrame
            DataFrame containing the annotations.
        """
        if not self.handler.is_connected:
            self.handler.connect()

        try:
            method = getattr(self.handler.api, 'children')
            result = method(parent_item_id, itemType='annotation')

            if isinstance(result, dict):
                return pd.DataFrame([result.get('data', {})])
            if isinstance(result, list) and all(isinstance(item, dict) for item in result):
                data_list = [item.get('data', {}) for item in result]
                return pd.DataFrame(data_list)

        except Exception as e:
            logger.error(f"Error fetching children for parent item ID {parent_item_id}: {e}")
            raise e

        return pd.DataFrame()

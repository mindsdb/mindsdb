from mindsdb_sql_parser import ast
from typing import Text, List, Dict, Any, Optional

from .exceptions import UnsupportedColumnException, MandatoryColumnException, ColumnCountMismatchException


class INSERTQueryParser:
    """
    Parses a INSERT query into its component parts.

    Parameters
    ----------
    query : ast.Insert
        Given SQL INSERT query.
    supported_columns : List[Text], Optional
        List of columns supported by the table for inserting.
    mandatory_columns : List[Text], Optional
        List of columns that must be present in the query for inserting.
    all_mandatory : Optional[Any], Optional (default=True)
        Whether all mandatory columns must be present in the query. If False, only one of the mandatory columns must be present.
    """
    def __init__(self, query: ast.Insert, supported_columns: Optional[List[Text]] = None, mandatory_columns: Optional[List[Text]] = None, all_mandatory: Optional[Any] = True):
        self.query = query
        self.supported_columns = supported_columns
        self.mandatory_columns = mandatory_columns
        self.all_mandatory = all_mandatory

    def parse_query(self) -> List[Dict[Text, Any]]:
        """
        Parses a SQL INSERT statement into its components: columns, values and returns a list of dictionaries with the values to insert.
        """
        columns = self.parse_columns()
        values = self.parse_values()

        values_to_insert = []
        for value in values:
            if len(columns) != len(value):
                raise ColumnCountMismatchException("Number of columns does not match the number of values")
            else:
                values_to_insert.append(dict(zip(columns, value)))

        return values_to_insert

    def parse_columns(self):
        """
        Parses the columns in the query. Raises an exception if the columns are not supported or if mandatory columns are missing.
        """
        columns = [col.name for col in self.query.columns]

        if self.supported_columns:
            if not set(columns).issubset(self.supported_columns):
                unsupported_columns = set(columns).difference(self.supported_columns)
                raise UnsupportedColumnException(f"Unsupported columns: {', '.join(unsupported_columns)}")

        if self.mandatory_columns:
            if self.all_mandatory:
                if not set(self.mandatory_columns).issubset(columns):
                    missing_mandatory_columns = set(self.mandatory_columns).difference(columns)
                    raise MandatoryColumnException(f"Mandatory columns missing: {', '.join(missing_mandatory_columns)}")
            else:
                if not set(self.mandatory_columns).intersection(columns):
                    missing_mandatory_columns = set(self.mandatory_columns).difference(columns)
                    raise MandatoryColumnException(f"Mandatory columns missing: {', '.join(missing_mandatory_columns)}")

        return columns

    def parse_values(self):
        """
        Parses the values in the query.
        """
        return self.query.values

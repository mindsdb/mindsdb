
class UnsupportedColumnException(Exception):
    """
    Exception raised when a column that is not supported is used in a query.
    """


class MandatoryColumnException(Exception):
    """
    Exception raised when a mandatory column is missing from a query.
    """


class ColumnCountMismatchException(Exception):
    """
    Exception raised when the number of columns in the query does not match the number of values.
    """

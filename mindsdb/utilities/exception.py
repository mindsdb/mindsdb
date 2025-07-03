from textwrap import indent


class BaseEntityException(Exception):
    """Base exception for entitys errors

    Attributes:
        message (str): error message
        entity_name (str): entity name
    """

    def __init__(self, message: str, entity_name: str = None) -> None:
        self.message = message
        self.entity_name = entity_name or "unknown"

    def __str__(self) -> str:
        return f"{self.message}: {self.entity_name}"


class EntityExistsError(BaseEntityException):
    """Raise when entity exists, but should not"""

    def __init__(self, message: str = None, entity_name: str = None) -> None:
        if message is None:
            message = "Entity exists error"
        super().__init__(message, entity_name)


class EntityNotExistsError(BaseEntityException):
    """Raise when entity not exists, but should"""

    def __init__(self, message: str = None, entity_name: str = None) -> None:
        if message is None:
            message = "Entity does not exists error"
        super().__init__(message, entity_name)


def format_db_error_message(
    db_name: str | None = None,
    db_type: str | None = None,
    db_error_msg: str | None = None,
    failed_query: str | None = None,
) -> str:
    """Format the error message for the database query.

    Args:
        db_name (str | None): The name of the database.
        db_type (str | None): The type of the database.
        db_error_msg (str | None): The error message.
        failed_query (str | None): The failed query.

    Returns:
        str: The formatted error message.
    """
    error_message = "Failed to execute external database query during query processing."
    if db_name is not None or db_type is not None:
        error_message += "\n\nDatabase Details:"
        if db_name is not None:
            error_message += f"\n- Name: {db_name}"
        if db_type is not None:
            error_message += f"\n- Type: {db_type}"

    if db_error_msg is not None:
        error_message += f"\n\nError:\n{indent(db_error_msg, '    ')}"

    if failed_query is not None:
        error_message += f"\n\nFailed Query:\n{indent(failed_query, '    ')}"

    return error_message

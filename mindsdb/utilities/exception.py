class BaseEntityException(Exception):
    """Base exception for entitys errors

        Attributes:
            message (str): error message
            entity_name (str): entity name
    """
    def __init__(self, message: str, entity_name: str = None) -> None:
        self.message = message
        self.entity_name = entity_name or 'unknown'

    def __str__(self) -> str:
        return f'{self.message}: {self.entity_name}'


class EntityExistsError(BaseEntityException):
    """Raise when entity exists, but should not"""
    def __init__(self, message: str = None, entity_name: str = None) -> None:
        if message is None:
            message = 'Entity exists error'
        super().__init__(message, entity_name)


class EntityNotExistsError(BaseEntityException):
    """Raise when entity not exists, but should"""
    def __init__(self, message: str = None, entity_name: str = None) -> None:
        if message is None:
            message = 'Entity does not exists error'
        super().__init__(message, entity_name)

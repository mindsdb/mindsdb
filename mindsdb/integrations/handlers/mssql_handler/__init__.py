from .mssql_handler import SqlServerHandler as Handler
from .__about__ import __version__ as version


name = 'mssql'

__all__ = ['Handler', 'version', 'name']

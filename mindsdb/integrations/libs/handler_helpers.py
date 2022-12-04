import importlib
# from pathlib import Path
# from mindsdb.integrations.handlers.mysql_handler.mysql_handler import MySQLHandler
# from mindsdb.integrations.handlers.mariadb_handler.mariadb_handler import MariaDBHandler
# from mindsdb.integrations.handlers.postgres_handler.postgres_handler import PostgresHandler

def get_handler(_type):
        # mindsdb_path = Path(importlib.util.find_spec('mindsdb').origin).parent
        # handlers_path = mindsdb_path.joinpath('integrations/handlers')
        handler_folder_name = _type + "_handler"
        try:
            handler_module = importlib.import_module(f'mindsdb.integrations.handlers.{handler_folder_name}')
            return handler_module.Handler
        except Exception as e:
            raise e


# def define_ml_handler(_type):
#     _type = _type.lower()
#     if _type == 'lightwood':
#         try:
#             from mindsdb.integrations.handlers.lightwood_handler.lightwood_handler.lightwood_handler import LightwoodHandler
#             return LightwoodHandler
#         except ImportError:
#             pass
#     elif _type == 'huggingface':
#         try:
#             from mindsdb.integrations.handlers.huggingface_handler.huggingface_handler import HuggingFaceHandler
#             return HuggingFaceHandler
#         except ImportError:
#             pass
#     return None
# 
# 
# def define_handler(_type):
#     _type = _type.lower()
#     if _type == 'mysql':
#         return MySQLHandler
#     if _type == 'postgres':
#         return PostgresHandler
#     if _type == 'mariadb':
#         return MariaDBHandler
#     return None

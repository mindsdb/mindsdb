# def native_query(self, query: Any) -> HandlerResponse:
#     """Receive raw query and act upon it somehow.
#     Args:
#         query (Any): query in native format (str for sql databases,
#             dict for mongo, etc)
#     Returns:
#         HandlerResponse
#     """
# def get_tables(self) -> HandlerResponse:
#     """ Return list of entities
#     Return list of entities that will be accesible as tables.
#     Returns:
#         HandlerResponse: shoud have same columns as information_schema.tables
#             (https://dev.mysql.com/doc/refman/8.0/en/information-schema-tables-table.html)
#             Column 'TABLE_NAME' is mandatory, other is optional.
#     """

# def get_columns(self, table_name: str) -> HandlerResponse:
#     """ Returns a list of entity columns
#     Args:
#         table_name (str): name of one of tables returned by self.get_tables()
#     Returns:
#         HandlerResponse: shoud have same columns as information_schema.columns
#             (https://dev.mysql.com/doc/refman/8.0/en/information-schema-columns-table.html)
#             Column 'COLUMN_NAME' is mandatory, other is optional. Hightly
#             recomended to define also 'DATA_TYPE': it should be one of
#             python data types (by default it str).
#     """

# 1. create a csv file
# 2. test is_it_parquet() with file, should return False
# 3. test is_it_xlsx() ...
# 4. test is_it_json() ...
# 5. test is_it_csv() should return True
# 6. get_tables()
# 7. get_columns()

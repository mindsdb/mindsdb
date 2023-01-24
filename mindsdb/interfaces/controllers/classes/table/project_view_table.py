from mindsdb_sql import parse_sql

from mindsdb.interfaces.controllers.abc.table import Table


class ProjectViewTable(Table):
    type = 'view'
    mysql_type = 'VIEW'
    mindsdb_type = 'VIEW'

    def from_record(record):
        view = ProjectViewTable()
        view.name = record.name
        view.id = record.id
        view._record = record
        view.metadata = {
            'id': record.id
        }
        return view

    def get_columns(self):
        # TODO
        return []

    def get_query_ast(self):
        query = self._record.query
        return parse_sql(query, dialect='mindsdb')

from mindsdb.interfaces.controllers.abc.table import Table


class ProjectDefaultTable(Table):
    type = 'default'
    mysql_type = 'BASE TABLE'
    mindsdb_type = 'BASE TABLE'

    def from_data(data):
        table = ProjectDefaultTable()
        table.name = data['name']
        return table

    def get_columns(self):
        if self.name == 'models':
            return []
        elif self.name == 'models_versions':
            return []
        else:
            raise Exception('TODO')

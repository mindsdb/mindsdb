import pandas

from mindsdb.api.mysql.mysql_proxy.datahub.datanodes.datanode import DataNode


class CSVDataNode(DataNode):
    type = 'csv'

    frames = {}

    def __init__(self, file_names=None):
        self.load(file_names)

    def load(self, file_names=None):
        if isinstance(file_names, dict):
            self.frames[file_names['name']] = pandas.read_csv(file_names['path'], dtype=str)
        elif isinstance(file_names, list):
            for fn in file_names:
                if isinstance(fn, dict):
                    self.frames[fn['name']] = pandas.read_csv(fn['path'], dtype=str)

    def getTables(self):
        return list(self.frames.keys())

    def hasTable(self, table):
        return table in self.frames

    def getTableColumns(self, table):
        return self.frames[table].columns.to_list()

    def _getPythonCondition(self, statement, operator=None):
        ops = {
            '$gt': '>',
            '$lt': '<',
            '$gte': '>=',
            '$lte': '<=',
            '$eq': '==',
            '$ne': '!=',
            '$in': 'in',
            '$or': 'or'
        }
        results = []
        for key, value in statement.items():
            result = ''
            if key in ops:
                if key == '$or':
                    ret = [self._getPythonCondition(v) for v in value]
                    ret = ' (' + ' or '.join(ret) + ') '
                    result += ret
                elif key == '$in':
                    # TODO
                    pass
                else:
                    result += ' ' + ops[key] + ' '
                    if isinstance(value, dict):
                        result += ' ' + self._getPythonCondition(value) + ' '
                    else:
                        if isinstance(value, str):
                            result += " '" + value + "' "
                        else:
                            result += ' ' + str(value) + ' '
            else:
                result += key
                if isinstance(value, dict):
                    result += ' ' + self._getPythonCondition(value) + ' '
                else:
                    if isinstance(value, str):
                        result += " '" + value + "' "
                    else:
                        result += ' ' + str(value) + ' '
            results.append(result)
        return ' and '.join(results)

    def select(self, table, columns=None, where=None, where_data=None, order_by=None, group_by=None):
        frame = self.frames[table]

        where_str = self._getPythonCondition(where, 'and')
        if len(where_str) > 0:
            frame = frame.query(where_str)

        return frame.to_dict('records')

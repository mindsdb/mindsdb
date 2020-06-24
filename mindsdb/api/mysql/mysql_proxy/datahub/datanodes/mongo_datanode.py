from pymongo import MongoClient
from mindsdb.api.mysql.mysql_proxy.datahub.datanodes.datanode import DataNode
from bson.objectid import ObjectId


class MongoDataNode(DataNode):
    type = 'mongo'

    def __init__(self, host, port, db_name):
        client = MongoClient(host, port)
        if db_name not in client.list_database_names():
            raise Exception(f'Mongo data base {db_name} not exists')
        self._db = client[db_name]

    def getTables(self):
        return self._db.collection_names()

    def hasTable(self, table):
        return table in self._db.collection_names()

    def getTableColumns(self, table):
        # return keys of first row
        return list(self._db[table].find_one().keys())

    def select(self, table, columns=None, where=None, where_data=None, order_by=None, group_by=None):
        selected_cols = {k: 1 for k in columns}

        for k, v in where.items():
            if k == '_id':
                where[k] = ObjectId(v)

        ret = []
        for row in self._db[table].find(where, selected_cols):
            for k, v in row.items():
                if isinstance(v, ObjectId):
                    row[k] = str(v)
            ret.append(row)
        return ret

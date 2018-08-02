"""
*******************************************************
 * Copyright (C) 2017 MindsDB Inc. <copyright@mindsdb.com>
 *
 * This file is part of MindsDB Server.
 *
 * MindsDB Server can not be copied and/or distributed without the express
 * permission of MindsDB Inc
 *******************************************************
"""

#TODO: Move this logic to drivers

import config as CONFIG
import jaydebeapi


CASTMAP = {
    'BOOLEAN': lambda x: bool(x),
    'DOUBLE': lambda x: float(x), # NOTE we may want to keep precision
    'INTEGER': lambda x: int(x)
}

class Drill:

    def __init__(self, use_schema = None):

        self.conn = jaydebeapi.connect("org.apache.drill.jdbc.Driver",
                         "jdbc:drill:drillbit={host}".format(host=CONFIG.DRILL_SERVER_HOST, port=CONFIG.DRILL_SERVER_PORT),
                          ["SA", ""],
                         CONFIG.DRILL_JDBC_DRIVER, )

        if use_schema is not None:
            st = self.conn.jconn.createStatement()
            st.executeQuery('use {schema}'.format(schema=use_schema))
            st.close()

    def query(self, sql, timeout = None):

        ret = {'header': [], 'header_types':[], 'data': []}

        st = self.conn.jconn.createStatement()
        rs = st.executeQuery(sql)
        meta = rs.getMetaData()
        for i in range(int(meta.columnCount)):
            ret['header'] += [meta.getColumnName(i+1)]
            ret['header_types'] += [meta.getColumnTypeName(i+1)]


        while rs.next():
            row = []
            for i in range(int(meta.columnCount)):
                cell_str = rs.getString(i+1)
                f = CASTMAP[ret['header_types'][i]]
                row += [f(rs.getString(i+1))]
            ret['data'] += [row]

        st.close()

        return ret




def test():
    d = Drill('Uploads.views')
    ret = d.query('select * from diamonds')
    print(ret)

# only run the test if this file is called from debugger
if __name__ == "__main__":
    test()



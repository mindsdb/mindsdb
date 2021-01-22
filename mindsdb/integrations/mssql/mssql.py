import pymssql
from mindsdb.integrations.base import Integration


class MSSQL(Integration):
    def _get_connnection(self):
        integration = self.config['integrations'][self.name]
        return pymssql.connect(
            server=integration['host'],
            host=integration['host'],
            user=integration['user'],
            password=integration['password'],
            database=integration.get('database', 'master'),
            port=integration['port'],
            autocommit=True  # that need for CRUD operations
        )

    def _query(self, query, fetch=False):
        conn = self._get_connnection()
        cur = conn.cursor(as_dict=True)
        cur.execute(query)
        res = True
        if fetch:
            res = cur.fetchall()
        cur.close()
        conn.close()
        return res

    def setup(self):
        integration = self.config['integrations'][self.name]
        driver_name = integration.get('odbc_driver_name', 'MySQL ODBC 8.0 Unicode Driver')
        servers = self._query('exec sp_linkedservers;', fetch=True)
        servers = [x['SRV_NAME'] for x in servers]
        if self.mindsdb_database in servers:
            self._query(f"exec sp_dropserver @server = N'{self.mindsdb_database}';")
        mysql = self.config['api']['mysql']
        self._query(f'''
            exec sp_addlinkedserver
                @server = N'{self.mindsdb_database}'
                ,@srvproduct=N'MySQL'
                ,@provider=N'MSDASQL'
                ,@provstr=N'DRIVER={{{driver_name}}}; SERVER={mysql['host']}; PORT={mysql['port']}; DATABASE=mindsdb; USER={mysql['user']}_{self.name}; {('PASSWORD=' + mysql['password'] + ';') if len(mysql['password']) > 0 else ''} OPTION=3;';
        ''')
        try:
            self._query(f"exec sp_serveroption @server='{self.mindsdb_database}', @optname='rpc', @optvalue='true'")
            self._query(f"exec sp_serveroption @server='{self.mindsdb_database}', @optname='rpc out', @optvalue='true'")
        except Exception:
            # nothing critical if server options not setted. Only 'four part' notation will not work.
            print('MSSQL integration: failed to set server options.')

    def register_predictors(self, model_data_arr):
        pass

    def unregister_predictor(self, name):
        pass

    def check_connection(self):
        try:
            conn = self._get_connnection()
            conn.close()
            connected = True
        except Exception:
            connected = False
        return connected

import pytds
from mindsdb.integrations.base import Integration


class MSSQL(Integration):
    def __init__(self, config, name):
        self.config = config
        self.name = name

    def _get_connnection(self):
        integration = self.config['integrations'][self.name]
        return pytds.connect(
            user=integration['user'],
            password=integration['password'],
            dsn=integration['host'],
            port=integration['port'],
            as_dict=True,
            autocommit=True     # connection.commit() not work
        )

    def _query(self, query, fetch=False):
        conn = self._get_connnection()
        cur = conn.cursor()
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
        if 'mindsdb' in servers:
            self._query("exec sp_dropserver @server = N'mindsdb';")
        mysql = self.config['api']['mysql']
        self._query(f'''
            exec sp_addlinkedserver
                @server = N'mindsdb'
                ,@srvproduct=N'MySQL'
                ,@provider=N'MSDASQL'
                ,@provstr=N'DRIVER={{{driver_name}}}; SERVER={mysql['host']}; PORT={mysql['port']}; DATABASE=mindsdb; USER={mysql['user']}_{self.name}; {('PASSWORD=' + mysql['password'] + ';') if len(mysql['password']) > 0 else ''} OPTION=3;';
        ''')

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

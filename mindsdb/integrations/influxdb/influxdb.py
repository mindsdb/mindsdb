from influxdb_client import InfluxDBClient
from influxdb_client.client.exceptions import InfluxDBError

# FIXME: Where is this?
from mindsdb.integrations.data_source import SQLDataSource
from mindsdb.utilities.log import log


class InfluxDS():
    """
    SQL Datasource class used for connections to InfluxDB
    """
    def __init__(self, **kwargs):
        self.protocol = kwargs.get('protocol')
        self.host = kwargs.get('host')
        self.port = kwargs.get('port')
        self.token = kwargs.get('token')
        self.org = kwargs.get('org')
        self.verify_ssl = kwargs.get('verify_ssl', True)
        self.ssl_ca_cert = kwargs.get('ssl_ca_cert')
        self.proxy = kwargs.get('proxy')
        self.proxy_headers = kwargs.get('proxy_headers')

        if self.protocol not in ('https', 'http'):
            raise ValueError('Unexpected protocol: {}'.fomat(self.protocol))

    def _get_connection(self):
        url = f'{self.protocol}://{self.host}:{self.port}'
        return InfluxDBClient(
            url,
            token=self.token,
            org=self.org,
            verify_ssl=self.verify_ssl,
            ssl_ca_cert=self.ssl_ca_cert,
            proxy=self.proxy,
            proxy_headers=self.proxy_headers
        )

    def query(self, q):
        try:
            with self._get_connection() as con:
                query_api = con.query_api()
                df = query_api.query_data_frame(q)
        except InfluxDBError as e:
            log.error(f'An unexpected error happend: {e}')
        return df, self._make_colmap(df)

    def check_connection(self):
        try:
            with self._get_connection() as con:
                connected = con.ping()
        except InfluxDBError as err:
            log.error(f'An unexpected error happend: {err}')
        return connected

    def name(self):
        return 'InfluxDB - {}'.format(self._query)

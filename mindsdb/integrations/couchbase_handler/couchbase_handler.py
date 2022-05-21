from datetime import timedelta

from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.diagnostics import PingState
from couchbase.options import ClusterOptions
from pandas import DataFrame

from mindsdb.api.mysql.mysql_proxy.mysql_proxy import RESPONSE_TYPE
from mindsdb.integrations.libs.base_handler import DatabaseHandler
from mindsdb.utilities.log import log


class CouchbaseHandler(DatabaseHandler):
    """
    Couchbase handler
    """

    def __init__(self, name, **kwargs):
        super().__init__(name)
        self.host = kwargs.get('host')
        self.username = kwargs.get('username')
        self.password = kwargs.get('password')
        self.cert_path = kwargs.get('cert_path')

    def __connect(self):
        """
        Handles the connection to a Couchbase server.
        """
        auth = PasswordAuthenticator(
            self.username,
            self.password,
            self.cert_path
        )

        cluster = Cluster(f'couchbase://{self.host}', ClusterOptions(auth))
        cluster.wait_until_ready(timedelta(seconds=5))

        return cluster

    def check_status(self):
        """
        Check the connection to the Couchbase server.
        :return: success status and error message if error occurs
        """
        status = {
            'success': False
        }
        try:
            cluster = self.__connect()
            ping_result = cluster.ping()

            service_availability = []
            for endpoint, reports in ping_result.endpoints.items():
                for report in reports:
                    if report.state == PingState.OK:
                        service_availability.append(True)

            if all(service_availability):
                status['sucess'] = True
            else:
                status['error'] = "Not all Couchbase services available."
        except Exception as e:
            log.error(f'Error connecting to Couchbase: {e}')
            status['error'] = e
        return status

    def native_query(self, query):
        """
        Receive a N1QL query and run it.
        :param query: The N1QL query to run
        :return: returns the records from the current recordset
        """
        cluster = self.__connect()

        try:
            query_result = cluster.query(query)

            rows = query_result.rows()

            if rows:
                df = DataFrame(query_result)

                response = {
                    'type': RESPONSE_TYPE.TABLE,
                    'data_frame': df
                }
            else:
                response = {
                    'type': RESPONSE_TYPE.OK
                }
        except Exception as e:
            log.error(f'Error running query: {query}')
            response = {
                'type': RESPONSE_TYPE.ERROR,
                'error_code': 0,
                'error_message': str(e)
            }

        return response

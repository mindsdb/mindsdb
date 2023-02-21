import numpy as np
import pandas as pd

from mindsdb.interfaces.controllers.classes.const import DatabaseType
from mindsdb.interfaces.controllers.abc.database import Database
from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE
from mindsdb.interfaces.database.integrations import IntegrationController


class IntegrationDB(Database):
    name: str
    engine: str
    type = DatabaseType.data

    def __init__(self):
        pass

    @staticmethod
    def from_record(record):
        integration = IntegrationDB()
        integration.name = record.name
        integration.engine = record.engine
        integration._record = record

        integration_controller = IntegrationController()
        integration.integration_handler = integration_controller.get_handler(integration.name)

        return integration

    def query(self, query=None, native_query=None):
        self.integration_handler.query(query)

        if query is not None:
            result = self.integration_handler.query(query)
        else:
            # try to fetch native query
            result = self.integration_handler.native_query(native_query)

        if result.type == RESPONSE_TYPE.ERROR:
            raise Exception(f'Error in {self.integration_name}: {result.error_message}')
        if result.type == RESPONSE_TYPE.OK:
            return

        df = result.data_frame
        df = df.replace(np.NaN, pd.NA).where(df.notnull(), None)
        columns_info = [
            {
                'name': k,
                'type': v
            }
            for k, v in df.dtypes.items()
        ]
        data = df.to_dict(orient='records')
        return data, columns_info

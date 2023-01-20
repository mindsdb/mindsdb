from mindsdb.interfaces.controllers.classes.const import DatabaseType
from mindsdb.interfaces.controllers.abc.database import Database


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
        return integration

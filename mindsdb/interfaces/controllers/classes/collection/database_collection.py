from mindsdb.interfaces.controllers.classes.const import DatabaseType
from mindsdb.interfaces.controllers.classes.collection.project_collection import ProjectCollection
from mindsdb.interfaces.controllers.classes.collection.integration_collection import IntegrationCollection


class DatabaseCollection():
    def __init__(self):
        # self._collection = []
        pass

    def all(self):
        raise NotImplementedError()

    def __contains__(self, name: str) -> bool:
        el = self.get(name)
        return el is not None

    def get(self, name: str, type: DatabaseType = None):  # -> Database
        integrations = IntegrationCollection().all()
        projects = ProjectCollection().all()
        # TODO InformationSchema!!!

        dbs = {
            x.name.casefold(): x for x in
            [*integrations, *projects]
        }

        db = dbs.get(name.casefold())

        if (
            db is not None and type is not None
            and db.type != type
        ):
            db = None

        return db

    @property
    def integrations(self):
        return IntegrationCollection()

    @property
    def projects(self):
        return ProjectCollection()

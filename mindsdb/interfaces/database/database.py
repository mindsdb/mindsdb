from typing import Optional
from collections import OrderedDict

from mindsdb.interfaces.database.projects import ProjectController
import mindsdb.utilities.profiler as profiler
from mindsdb.utilities.exception import EntityNotExistsError
from mindsdb.interfaces.database.log import LogDBController


class DatabaseController:
    def __init__(self):
        from mindsdb.interfaces.database.integrations import integration_controller
        self.integration_controller = integration_controller
        self.project_controller = ProjectController()

        self.logs_db_controller = LogDBController()
        self.information_schema_controller = None

    def delete(self, name: str):
        databases = self.get_dict()
        name = name.lower()
        if name not in databases:
            raise EntityNotExistsError('Database does not exists', name)
        db_type = databases[name]['type']
        if db_type == 'project':
            project = self.get_project(name)
            project.delete()
            return
        elif db_type == 'data':
            self.integration_controller.delete(name)
            return
        else:
            raise Exception(f"Database with type '{db_type}' cannot be deleted")

    @profiler.profile()
    def get_list(self, filter_type: Optional[str] = None, with_secrets: Optional[bool] = True):
        projects = self.project_controller.get_list()
        integrations = self.integration_controller.get_all(show_secrets=with_secrets)
        result = [{
            'name': 'information_schema',
            'type': 'system',
            'id': None,
            'engine': None,
            'visible': True,
            'deletable': False
        }, {
            'name': 'log',
            'type': 'system',
            'id': None,
            'engine': None,
            'visible': True,
            'deletable': False
        }]
        for x in projects:
            result.append({
                'name': x.name,
                'type': 'project',
                'id': x.id,
                'engine': None,
                'visible': True,
                'deletable': x.name.lower() != 'mindsdb'
            })
        for key, value in integrations.items():
            db_type = value.get('type', 'data')
            if db_type != 'ml':
                result.append({
                    'name': key,
                    'type': value.get('type', 'data'),
                    'id': value.get('id'),
                    'engine': value.get('engine'),
                    'class_type': value.get('class_type'),
                    'connection_data': value.get('connection_data'),
                    'visible': True,
                    'deletable': value.get('permanent', False) is False
                })

        if filter_type is not None:
            result = [x for x in result if x['type'] == filter_type]

        return result

    def get_dict(self, filter_type: Optional[str] = None):
        return OrderedDict(
            (
                x['name'].lower(),
                {
                    'type': x['type'],
                    'engine': x['engine'],
                    'id': x['id']
                }
            )
            for x in self.get_list(filter_type=filter_type)
        )

    def get_integration(self, integration_id):
        # get integration by id

        # TODO get directly from db?
        for rec in self.get_list():
            if rec['id'] == integration_id and rec['type'] == 'data':
                return {
                    'name': rec['name'],
                    'type': rec['type'],
                    'engine': rec['engine'],
                    'id': rec['id']
                }

    def exists(self, db_name: str) -> bool:
        return db_name in self.get_dict()

    def get_project(self, name: str):
        return self.project_controller.get(name=name)

    def get_system_db(self, name: str):
        if name == 'log':
            return self.logs_db_controller
        elif name == 'information_schema':
            from mindsdb.api.executor.controllers.session_controller import SessionController
            session = SessionController()
            return session.datahub
        else:
            raise Exception(f"Database '{name}' does not exists")

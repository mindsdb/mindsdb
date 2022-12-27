from typing import Optional
from collections import OrderedDict

from mindsdb.interfaces.database.integrations import IntegrationController
from mindsdb.interfaces.database.projects import ProjectController


class DatabaseController:
    def __init__(self):
        self.integration_controller = IntegrationController()
        self.project_controller = ProjectController()

    def delete(self, name: str):
        databases = self.get_dict()
        if name not in databases:
            raise Exception(f"Database '{name}' does not exists")
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

    def get_list(self, filter_type: Optional[str] = None):
        projects = self.project_controller.get_list()
        integrations = self.integration_controller.get_all()
        result = [{
            'name': 'information_schema',
            'type': 'system',
            'id': None,
            'engine': None
        }]
        for x in projects:
            result.append({
                'name': x.name,
                'type': 'project',
                'id': x.id,
                'engine': None
            })
        for key, value in integrations.items():
            db_type = value.get('type', 'data')
            if db_type != 'ml':
                result.append({
                    'name': key,
                    'type': value.get('type', 'data'),
                    'id': value.get('id'),
                    'engine': value.get('engine')
                })

        if filter_type is not None:
            result = [x for x in result if x['type'] == filter_type]

        return result

    def get_dict(self, filter_type: Optional[str] = None):
        return OrderedDict(
            (
                x['name'],
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

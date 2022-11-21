from typing import Optional
from collections import OrderedDict

from mindsdb.interfaces.database.integrations import IntegrationController
from mindsdb.interfaces.database.projects import ProjectController


class DatabaseController:
    def __init__(self):
        self.integration_controller = IntegrationController()
        self.project_controller = ProjectController()

    def delete(self, name: str, company_id: Optional[int]):
        databases = self.get_dict(company_id=company_id)
        if name not in databases:
            raise Exception(f"Database '{name}' does not exists")
        db_type = databases[name]['type']
        if db_type == 'project':
            project = self.get_project(name, company_id=company_id)
            project.delete()
            return
        elif db_type == 'data':
            self.integration_controller.delete(name)
            return
        else:
            raise Exception(f"Database with type '{db_type}' cannot be deleted")

    def get_list(self, company_id: Optional[int], filter_type: Optional[str] = None):
        projects = self.project_controller.get_list(company_id=company_id)
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

    def get_dict(self, company_id: Optional[int], filter_type: Optional[str] = None):
        return OrderedDict(
            (
                x['name'],
                {
                    'type': x['type'],
                    'engine': x['engine'],
                    'id': x['id']
                }
            )
            for x in self.get_list(company_id=company_id, filter_type=filter_type)
        )

    def get_integration(self, integration_id, company_id=None):
        # get integration by id

        # TODO get directly from db?
        for rec in self.get_list(company_id=company_id):
            if rec['id'] == integration_id and rec['type'] == 'data':
                return {
                    'name': rec['name'],
                    'type': rec['type'],
                    'engine': rec['engine'],
                    'id': rec['id']
                }

    def exists(self, db_name: str, company_id: Optional[int]) -> bool:
        return db_name in self.get_dict(company_id=company_id)

    def get_project(self, name: str, company_id: Optional[int]):
        return self.project_controller.get(name=name, company_id=company_id)

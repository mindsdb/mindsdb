from collections import OrderedDict

from mindsdb.interfaces.database.integrations import IntegrationController
from mindsdb.interfaces.database.projects import ProjectController


class DatabaseController:
    def __init__(self):
        self.integration_controller = IntegrationController()
        self.project_controller = ProjectController()

    # def create(self): ...
    # def delete(self): ...

    def get_list(self, company_id: int, filter_type: str = None):
        projects = self.project_controller.get_list(company_id=company_id)
        integrations = self.integration_controller.get_all(company_id=company_id)
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
            result.append({
                'name': key,
                'type': 'data',
                'id': value.get('id'),
                'engine': value.get('engine')
            })

        if filter_type is not None:
            result = [x for x in result if x['type'] == filter_type]

        return result

    def get_dict(self, company_id: int, filter_type: str = None):
        return OrderedDict(
            (x['name'], {'type': x['type']})
            for x in self.get_list(company_id=company_id, filter_type=filter_type)
        )

    def get_project(self, name):
        return self.project_controller.get(name=name)

    # def get_tables(self, db): ...
    # def get_column(self, db, table): ...

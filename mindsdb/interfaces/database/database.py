from mindsdb.interfaces.database.integrations import IntegrationController
from mindsdb.interfaces.database.projects import ProjectController


class DatabaseController:
    def __init__(self, integration_controller=None, project_controller=None):
        self.integration_controller = IntegrationController()
        self.project_controller = ProjectController()

    def create(self): ...
    def delete(self): ...
    def get_list(self, company_id):
        projects = self.project_controller.get_list(company_id=company_id)
        integrations = self.integration_controller.get_all(company_id=company_id)
        result = [{'name': 'information_schema', 'type': 'system'}]
        for x in projects:
            result.append({'name': x.name, 'type': 'project'})
        for x in integrations.keys():
            result.append({'name': x, 'type': 'integration'})
        return result

    def get_tables(self, db): ...
    def get_column(self, db, table): ...

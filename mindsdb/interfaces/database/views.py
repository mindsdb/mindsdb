from mindsdb.interfaces.storage.db import session, View, Project


class ViewController:
    def add(self, name, query, project_name, company_id=None):
        from mindsdb.interfaces.database.database import DatabaseController

        database_controller = DatabaseController()
        project_databases_dict = database_controller.get_dict(company_id=company_id, filter_type='project')

        if project_name not in project_databases_dict:
            raise Exception(f"Can not find project: '{project_name}'")

        project_id = project_databases_dict[project_name]['id']
        view_record = (
            session.query(View.id)
            .filter_by(
                name=name,
                company_id=company_id,
                project_id=project_id
            ).first()
        )
        if view_record is not None:
            raise Exception(f'View already exists: {name}')

        view_record = View(
            name=name,
            company_id=company_id,
            query=query,
            project_id=project_id
        )
        session.add(view_record)
        session.commit()

    def delete(self, name, project_name, company_id=None):
        project_record = session.query(Project).filter_by(
            name=project_name,
            company_id=company_id,
            deleted_at=None
        ).first()
        rec = session.query(View).filter(
            View.name == name,
            View.company_id == company_id,
            View.project_id == project_record.id
        ).first()
        if rec is None:
            raise Exception(f'View not found: {name}')
        session.delete(rec)
        session.commit()

    def _get_view_record_data(self, record):
        return {
            'name': record.name,
            'query': record.query
        }

    def get(self, id=None, name=None, project_name=None, company_id=None):
        project_record = session.query(Project).filter_by(
            name=project_name,
            company_id=company_id,
            deleted_at=None
        ).first()
        if id is not None:
            records = session.query(View).filter_by(id=id, project_id=project_record.id, company_id=company_id).all()
        elif name is not None:
            records = session.query(View).filter_by(name=name, project_id=project_record.id, company_id=company_id).all()
        if len(records) == 0:
            raise Exception(f"Can't find view with name/id: {name}/{id}")
        elif len(records) > 1:
            raise Exception(f"There are multiple views with name/id: {name}/{id}")
        record = records[0]
        return self._get_view_record_data(record)

    def get_all(self, company_id=None):
        view_records = session.query(View).filter_by(company_id=company_id).all()
        views_dict = {}
        for record in view_records:
            views_dict[record.name] = self._get_view_record_data(record)
        return views_dict

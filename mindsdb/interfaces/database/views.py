from mindsdb.interfaces.storage import db
from mindsdb.utilities.context import context as ctx


class ViewController:
    def add(self, name, query, project_name):
        from mindsdb.interfaces.database.database import DatabaseController

        database_controller = DatabaseController()
        project_databases_dict = database_controller.get_dict(filter_type='project')

        if project_name not in project_databases_dict:
            raise Exception(f"Can not find project: '{project_name}'")

        project_id = project_databases_dict[project_name]['id']
        view_record = (
            db.session.query(db.View.id)
            .filter_by(
                name=name,
                company_id=ctx.company_id,
                project_id=project_id
            ).first()
        )
        if view_record is not None:
            raise Exception(f'View already exists: {name}')

        view_record = db.View(
            name=name,
            company_id=ctx.company_id,
            query=query,
            project_id=project_id
        )
        db.session.add(view_record)
        db.session.commit()

    def delete(self, name, project_name):
        project_record = db.session.query(db.Project).filter_by(
            name=project_name,
            company_id=ctx.company_id,
            deleted_at=None
        ).first()
        rec = db.session.query(db.View).filter(
            db.View.name == name,
            db.View.company_id == ctx.company_id,
            db.View.project_id == project_record.id
        ).first()
        if rec is None:
            raise Exception(f'View not found: {name}')
        db.session.delete(rec)
        db.session.commit()

    def _get_view_record_data(self, record):
        return {
            'name': record.name,
            'query': record.query
        }

    def get(self, id=None, name=None, project_name=None):
        project_record = db.session.query(db.Project).filter_by(
            name=project_name,
            company_id=ctx.company_id,
            deleted_at=None
        ).first()
        if id is not None:
            records = db.session.query(db.View).filter_by(
                id=id,
                project_id=project_record.id,
                company_id=ctx.company_id
            ).all()
        elif name is not None:
            records = db.session.query(db.View).filter_by(
                name=name,
                project_id=project_record.id,
                company_id=ctx.company_id
            ).all()
        if len(records) == 0:
            raise Exception(f"Can't find view with name/id: {name}/{id}")
        elif len(records) > 1:
            raise Exception(f"There are multiple views with name/id: {name}/{id}")
        record = records[0]
        return self._get_view_record_data(record)

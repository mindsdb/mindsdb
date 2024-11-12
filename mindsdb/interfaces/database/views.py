from sqlalchemy import func
from mindsdb.interfaces.storage import db
from mindsdb.interfaces.query_context.context_controller import query_context_controller
from mindsdb.utilities.context import context as ctx
from mindsdb.utilities.exception import EntityExistsError, EntityNotExistsError


class ViewController:
    def add(self, name, query, project_name):
        name = name.lower()
        from mindsdb.interfaces.database.database import DatabaseController

        database_controller = DatabaseController()
        project_databases_dict = database_controller.get_dict(filter_type='project')

        if project_name not in project_databases_dict:
            raise EntityNotExistsError('Can not find project', project_name)

        project_id = project_databases_dict[project_name]['id']
        view_record = (
            db.session.query(db.View.id)
            .filter(
                func.lower(db.View.name) == name,
                db.View.company_id == ctx.company_id,
                db.View.project_id == project_id
            ).first()
        )
        if view_record is not None:
            raise EntityExistsError('View already exists', name)

        view_record = db.View(
            name=name,
            company_id=ctx.company_id,
            query=query,
            project_id=project_id
        )
        db.session.add(view_record)
        db.session.commit()

    def update(self, name, query, project_name):
        name = name.lower()
        project_record = db.session.query(db.Project).filter_by(
            name=project_name,
            company_id=ctx.company_id,
            deleted_at=None
        ).first()
        rec = db.session.query(db.View).filter(
            func.lower(db.View.name) == name,
            db.View.company_id == ctx.company_id,
            db.View.project_id == project_record.id
        ).first()
        if rec is None:
            raise EntityNotExistsError('View not found', name)
        rec.query = query
        db.session.commit()

    def delete(self, name, project_name):
        name = name.lower()
        project_record = db.session.query(db.Project).filter_by(
            name=project_name,
            company_id=ctx.company_id,
            deleted_at=None
        ).first()
        rec = db.session.query(db.View).filter(
            func.lower(db.View.name) == name,
            db.View.company_id == ctx.company_id,
            db.View.project_id == project_record.id
        ).first()
        if rec is None:
            raise EntityNotExistsError('View not found', name)
        db.session.delete(rec)
        db.session.commit()

        query_context_controller.drop_query_context('view', rec.id)

    def list(self, project_name):
        query = db.session.query(db.Project).filter_by(
            company_id=ctx.company_id,
            deleted_at=None
        )
        if project_name is not None:
            query = query.filter_by(name=project_name)

        project_names = {
            i.id: i.name
            for i in query
        }

        query = db.session.query(db.View).filter(
            db.View.company_id == ctx.company_id,
            db.View.project_id.in_(list(project_names.keys()))
        )

        data = []

        for record in query:

            data.append({
                'id': record.id,
                'name': record.name,
                'project': project_names[record.project_id],
                'query': record.query,
            })

        return data

    def _get_view_record_data(self, record):
        return {
            'id': record.id,
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
            records = db.session.query(db.View).filter(
                func.lower(db.View.name) == name.lower(),
                db.View.project_id == project_record.id,
                db.View.company_id == ctx.company_id,
            ).all()
        if len(records) == 0:
            if name is None:
                name = f'id={id}'
            raise EntityNotExistsError("Can't find view", f'{project_name}.{name}')
        elif len(records) > 1:
            raise Exception(f"There are multiple views with name/id: {name}/{id}")
        record = records[0]
        return self._get_view_record_data(record)

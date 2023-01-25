import sqlalchemy as sa

from mindsdb.interfaces.controllers.abc.collection import Collection
from mindsdb.interfaces.controllers.classes.database.project_db import ProjectDB
from mindsdb.interfaces.storage import db
from mindsdb.utilities.context import context as ctx


class ProjectCollection(Collection):
    def __init__(self):
        pass

    def all(self):
        records = db.Project.query.filter(
            (db.Project.company_id == ctx.company_id)
            & (db.Project.deleted_at == sa.null())
        ).order_by(db.Project.name)

        return [ProjectDB.from_record(r) for r in records]

    def __contains__(self, key):
        raise NotImplementedError()

    def __delitem__(self, key):
        raise NotImplementedError()

    def __getitem__(self, key):
        raise NotImplementedError()

    def __setitem__(self, key, value):
        raise NotImplementedError()

    def get(self, name):
        record = db.Project.query.filter(
            (db.Project.company_id == ctx.company_id)
            & (db.Project.deleted_at == sa.null())
            & (sa.func.lower(db.Project.name) == name.lower())
        ).order_by(db.Project.name).first()

        if record is None:
            return None

        return ProjectDB.from_record(record)

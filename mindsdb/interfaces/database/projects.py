import time
from typing import List
from collections import OrderedDict

import sqlalchemy as sa

from mindsdb.interfaces.storage import db


class Project:
    @staticmethod
    def from_record(db_record: db.Project):
        p = Project()
        p.record = db_record
        p.name = db_record.name
        p.company_id = db_record.company_id
        p.id = db_record.id
        return p

    def create(self, name: str, company_id: int):
        existing_record = db.Project.query.filter(
            (db.Project.name == name)
            & (db.Project.company_id == company_id)
            & (db.Project.deleted_at == sa.null())
        ).first()
        if existing_record is not None:
            raise Exception(f"Project with name '{name}' already exists")

        record = db.Project(
            name=name,
            company_id=company_id
        )

        self.record = record
        self.name = name
        self.company_id = company_id

        db.session.add(record)
        db.session.commit()

        self.id = record.id

    def save(sefl):
        db.session.commit()

    def delete(self):
        self.record.deleted_at = time.time()
        db.session.commit()

    def get_tables(self):
        records = db.session.query(db.Predictor, db.Integration).filter_by(
            project_id=self.id,
            deleted_at=sa.null(),
            company_id=self.company_id,
            active=True
        ).join(db.Integration, db.Integration.id == db.Predictor.integration_id).all()

        return OrderedDict(
            (
                predictor_record.name,
                {
                    'type': 'model',
                    'engine': integraion_record.engine,
                    'engine_name': integraion_record.name
                }
            )
            for predictor_record, integraion_record in records
        )

    def get_columns(self, table_name: str):
        # at the moment it works only for models
        predictor_record = db.Predictor.query.filter_by(
            company_id=self.company_id,
            project_id=self.id,
            name=table_name
        ).first()
        columns = []
        if predictor_record is not None and isinstance(predictor_record.dtype_dict, dict):
            columns = list(predictor_record.dtype_dict.keys())

        return columns


class ProjectController:
    def __init__(self):
        pass

    def get_list(self, company_id: int = None) -> List[Project]:
        records = db.Project.query.filter(
            (db.Project.company_id == company_id)
            & (db.Project.deleted_at == sa.null())
        ).order_by(db.Project.name)

        return [Project.from_record(x) for x in records]

    def get(self, id: int = None, name: str = None, deleted: bool = False, company_id: int = None) -> Project:
        if id is not None and name is not None:
            raise ValueError("Both 'id' and 'name' is None")

        q = db.Project.query.filter_by(company_id=company_id)

        if id is not None:
            q = q.filter_by(id=id)
        elif name is not None:
            q = q.filter(
                (sa.func.lower(db.Project.name) == sa.func.lower(name))
            )

        if deleted is True:
            q.filter((db.Project.deleted_at != sa.null()))
        else:
            q.filter_by(deleted_at=sa.null())

        record = q.one()

        return Project.from_record(record)

    def add(self, name: str, company_id: int = None) -> Project:
        project = Project()
        project.create(name=name, company_id=company_id)
        return project

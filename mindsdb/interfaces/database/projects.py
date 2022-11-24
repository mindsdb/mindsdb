import datetime
from typing import List
from copy import deepcopy
from collections import OrderedDict

import sqlalchemy as sa
import numpy as np

from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql import parse_sql

from mindsdb.interfaces.storage import db
from mindsdb.utilities.config import Config
from mindsdb.interfaces.model.model_controller import ModelController
from mindsdb.interfaces.database.views import ViewController
from mindsdb.utilities.context import context as ctx


class Project:
    @staticmethod
    def from_record(db_record: db.Project):
        p = Project()
        p.record = db_record
        p.name = db_record.name
        p.company_id = db_record.company_id
        p.id = db_record.id
        return p

    def create(self, name: str):
        existing_record = db.Project.query.filter(
            (db.Project.name == name)
            & (db.Project.company_id == ctx.company_id)
            & (db.Project.deleted_at == sa.null())
        ).first()
        if existing_record is not None:
            raise Exception(f"Project with name '{name}' already exists")

        record = db.Project(
            name=name,
            company_id=ctx.company_id
        )

        self.record = record
        self.name = name
        self.company_id = ctx.company_id

        db.session.add(record)
        db.session.commit()

        self.id = record.id

    def save(sefl):
        db.session.commit()

    def delete(self):
        tables = self.get_tables()
        tables = [key for key, val in tables.items() if val['type'] != 'table']
        if len(tables) > 0:
            raise Exception(f"Project '{self.name}' can not be deleted, because it contains tables: {', '.join(tables)}")

        is_cloud = Config().get('cloud', False)
        if is_cloud is True:
            self.record.deleted_at = datetime.datetime.now()
        else:
            db.session.delete(self.record)
            self.record = None
            self.name = None
            self.company_id = None
            self.id = None
        db.session.commit()

    def drop_table(self, table_name: str):
        tables = self.get_tables()
        if table_name not in tables:
            raise Exception(f"Table '{table_name}' do not exists")
        table_meta = tables[table_name]
        if table_meta['type'] == 'model':
            ModelController().delete_model(
                table_name,
                project_name=self.name
            )
        elif table_meta['type'] == 'view':
            ViewController().delete(
                table_name,
                project_name=self.name
            )
        else:
            raise Exception(f"Can't delete table '{table_name}' because of it type: {table_meta['type']}")

    def create_view(self, name: str, query: str):
        ViewController().add(
            name,
            query=query,
            project_name=self.name
        )

    def query_view(self, query: ASTNode) -> ASTNode:
        view_name = query.from_table.parts[-1]
        view_meta = ViewController().get(
            name=view_name,
            project_name=self.name
        )
        subquery_ast = parse_sql(view_meta['query'], dialect='mindsdb')
        return subquery_ast

    def get_models(self):
        records = (
            db.session.query(db.Predictor, db.Integration).filter_by(
                project_id=self.id,
                deleted_at=sa.null(),
                company_id=ctx.company_id
            )
            .join(db.Integration, db.Integration.id == db.Predictor.integration_id)
            .order_by(db.Predictor.name, db.Predictor.id)
            .all()
        )

        data = []

        for predictor_record, integraion_record in records:
            predictor_data = deepcopy(predictor_record.data) or {}
            predictor_meta = {
                'type': 'model',
                'id': predictor_record.id,
                'engine': integraion_record.engine,
                'engine_name': integraion_record.name,
                'active': predictor_record.active,
                'version': predictor_record.version,
                'status': predictor_record.status,
                'accuracy': None,
                'predict': predictor_record.to_predict[0],
                'update_status': predictor_record.update_status,
                'mindsdb_version': predictor_record.mindsdb_version,
                'error': predictor_data.get('error'),
                'select_data_query': predictor_record.fetch_data_query,
                'training_options': predictor_record.learn_args,
                'deletable': True,
                'label': predictor_record.label,
            }
            if predictor_data is not None and predictor_data.get('accuracies', None) is not None:
                if len(predictor_data['accuracies']) > 0:
                    predictor_meta['accuracy'] = float(np.mean(list(predictor_data['accuracies'].values())))
            data.append({'name': predictor_record.name, 'metadata': predictor_meta})

        return data

    def get_views(self):
        records = (
            db.session.query(db.View).filter_by(
                project_id=self.id,
                company_id=ctx.company_id
            )
            .order_by(db.View.name, db.View.id)
            .all()
        )
        data = [{
            'name': view_record.name,
            'metadata': {
                'type': 'view',
                'id': view_record.id,
                'deletable': True
            }}
            for view_record in records
        ]
        return data

    def get_tables(self):
        data = OrderedDict()
        data['models'] = {'type': 'table', 'deletable': False}
        data['models_versions'] = {'type': 'table', 'deletable': False}

        models = self.get_models()
        for model in models:
            if model['metadata']['active'] is True:
                data[model['name']] = model['metadata']

        views = self.get_views()
        for view in views:
            data[view['name']] = view['metadata']

        return data

    def get_columns(self, table_name: str):
        # at the moment it works only for models
        predictor_record = db.Predictor.query.filter_by(
            company_id=ctx.company_id,
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

    def get_list(self) -> List[Project]:
        records = db.Project.query.filter(
            (db.Project.company_id == ctx.company_id)
            & (db.Project.deleted_at == sa.null())
        ).order_by(db.Project.name)

        return [Project.from_record(x) for x in records]

    def get(self, id: int = None, name: str = None, deleted: bool = False) -> Project:
        if id is not None and name is not None:
            raise ValueError("Both 'id' and 'name' is None")

        q = db.Project.query.filter_by(company_id=ctx.company_id)

        if id is not None:
            q = q.filter_by(id=id)
        elif name is not None:
            q = q.filter(
                (sa.func.lower(db.Project.name) == sa.func.lower(name))
            )

        if deleted is True:
            q = q.filter((db.Project.deleted_at != sa.null()))
        else:
            q = q.filter_by(deleted_at=sa.null())

        record = q.one()

        return Project.from_record(record)

    def add(self, name: str) -> Project:
        project = Project()
        project.create(name=name)
        return project

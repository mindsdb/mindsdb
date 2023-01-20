from copy import deepcopy
from collections import OrderedDict
from typing import List, Union

import sqlalchemy as sa
import numpy as np

from mindsdb.interfaces.storage import db
from mindsdb.utilities.context import context as ctx
from mindsdb.interfaces.controllers.classes.const import DatabaseType
from mindsdb.interfaces.controllers.classes.table.project_default_table import ProjectDefaultTable
from mindsdb.interfaces.controllers.classes.table.project_model_table import ProjectModelTable
from mindsdb.interfaces.controllers.classes.table.project_view_table import ProjectViewTable
from mindsdb.interfaces.controllers.abc.database import Database


class ProjectDB(Database):
    name: str
    type = DatabaseType.project

    def __init__(self):
        pass

    @staticmethod
    def from_record(record):
        project = ProjectDB()
        project.name = record.name
        project._record = record
        return project

    def all(self) -> List[Union[ProjectDefaultTable, ProjectModelTable, ProjectViewTable]]:
        # data = OrderedDict()
        # data.update(self._get_default())
        # data.update(self._get_models())
        # data.update(self._get_views())
        data = [
            *self._get_default(),
            *self._get_models(),
            *self._get_views()
        ]
        return data

    def get(self, name: str):  # -> table
        data = self.all()
        return data.get(name)

    def _get_models(self):
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
            data.append(
                ProjectModelTable.from_record(predictor_record, integraion_record)
            )

        return data

    def _get_views(self):
        records = (
            db.session.query(db.View).filter_by(
                project_id=self.id,
                company_id=ctx.company_id
            )
            .order_by(db.View.name, db.View.id)
            .all()
        )

        data = [
            ProjectViewTable(record) for record in records
        ]

        return data

    def _get_default(self):
        data = [
            ProjectDefaultTable({'name': 'models'}),
            ProjectDefaultTable({'name': 'models_versions'})
        ]
        return data
        # data = OrderedDict()
        # data['models'] = {'type': 'table', 'deletable': False}
        # data['models_versions'] = {'type': 'table', 'deletable': False}
        # return data

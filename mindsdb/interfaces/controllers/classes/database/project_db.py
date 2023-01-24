from typing import List, Union

import sqlalchemy as sa

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
        project.id = record.id
        return project

    def get_tables(self):
        return self.all()

    def all(self) -> List[Union[ProjectDefaultTable, ProjectModelTable, ProjectViewTable]]:
        data = [
            *self._get_default(),
            *self._get_models(),
            *self._get_views()
        ]
        return data

    def get(self, name: str):  # -> table
        # region check is 'default' table
        default_table = next((
            x for x in self._get_default()
            if x.name.casefold() == name.casefold()
        ), None)
        if default_table is not None:
            return default_table
        # endregion

        # region check is model
        record = (
            db.session.query(db.Predictor, db.Integration).filter(
                (sa.func.lower(db.Predictor.name) == name.lower())
                & (db.Predictor.project_id == self.id)
                & (db.Predictor.deleted_at == sa.null())
                & (db.Predictor.company_id == ctx.company_id)
            )
            .join(db.Integration, db.Integration.id == db.Predictor.integration_id)
            .order_by(db.Predictor.name, db.Predictor.id)
            .first()
        )

        if record is not None:
            return ProjectModelTable.from_record(record[0], record[1])
        # endregion

        # region check is view
        record = (
            db.session.query(db.View).filter(
                (sa.func.lower(db.View.name) == name.lower())
                & (db.View.project_id == self.id)
                & (db.View.company_id == ctx.company_id)
            )
            .order_by(db.View.name, db.View.id)
            .first()
        )

        if record is not None:
            return ProjectViewTable.from_record(record)
        # endregion

        return None

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
            ProjectViewTable.from_record(record) for record in records
        ]

        return data

    def _get_default(self):
        data = [
            ProjectDefaultTable.from_data({'name': 'models'}),
            ProjectDefaultTable.from_data({'name': 'models_versions'})
        ]
        return data

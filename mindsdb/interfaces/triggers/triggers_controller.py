
import re
import datetime as dt
from dateutil.relativedelta import relativedelta

import sqlalchemy as sa

from mindsdb_sql.parser.ast import Data, Identifier

from mindsdb_sql import parse_sql, ParsingException

from mindsdb.interfaces.storage import db
from mindsdb.interfaces.database.projects import ProjectController
from mindsdb.utilities.context import context as ctx
from mindsdb.utilities import log
from mindsdb.utilities.config import Config


class TriggersController:
    OBJECT_TYPE = 'trigger'

    def add(self, name, project_name, table, query_str):
        name = name.lower()

        if project_name is None:
            project_name = 'mindsdb'
        project_controller = ProjectController()
        project = project_controller.get(name=project_name)

        from mindsdb.api.mysql.mysql_proxy.controllers.session_controller import SessionController
        session = SessionController()

        # check exists
        all_triggers = self.get_list(project_name)
        if any([i for i in all_triggers if i['name'] == name]):
            raise Exception(f'Trigger already exists: {name}')

        # check table
        if len(table.parts) < 2:
            raise Exception(f'Database or table not found: {table}')

        table_name = Identifier(parts=table.parts[1:]).to_string()
        db_name = table.parts[1]

        db_handler = session.integration_controller.get_handler(db_name)
        df = db_handler.get_tables().data_frame
        tables = df[df.columns[0]]
        if not table_name in tables:
            raise Exception(f'Table {table_name} not found in {db_name}')

        # check sql
        try:
            parse_sql(query_str, dialect='mindsdb')
        except ParsingException as e:
            raise ParsingException(f'Unable to parse: {query_str}: {e}')

        # create job record
        record = db.Triggers(
            name=name,
            project_id=project.id,

            database_id=db_handler['id'],
            table_name=table_name,
            query_str=query_str,
        )
        db.session.add(record)
        db.session.commit()

    def delete(self, name, project_name):
        # check exists
        all_triggers = self.get_list(project_name)
        if not any([i for i in all_triggers if i['name'] == name]):
            raise Exception(f"Trigger already doesn't exist: {name}")

        project_controller = ProjectController()
        project = project_controller.get(name=project_name)

        trigger = db.Triggers.query.filter(
            db.Triggers.name == name,
            db.Triggers.project_id == project.id
        ).first()

        task = db.Tasks.query.filter(
            db.Tasks.object_type == self.OBJECT_TYPE,
            db.Tasks.object_id == trigger.id
        )

        db.session.delete(task)
        db.session.delete(trigger)

        db.session.commit()

    def get_list(self, project_name=None):
        from mindsdb.api.mysql.mysql_proxy.controllers.session_controller import SessionController
        session = SessionController()

        query = db.session.query(
            db.Tasks.object_id,
            db.Triggers.project_id,
            db.Triggers.name,
            db.Triggers.database_id,
            db.Triggers.table_name,
            db.Triggers.query_str,
            db.Triggers.last_error,
        )\
            .join(db.Triggers, db.Triggers.id == db.Tasks.object_id)\
            .filter(
                db.Tasks.object_type == self.OBJECT_TYPE,
                db.Tasks.company_id == ctx.company_id,
        )

        project_controller = ProjectController()
        if project_name is not None:
            project = project_controller.get(name=project_name)
            query = query.filter(db.Triggers.project_id == project.id)

        database_names = {
            i['id']: i['name']
            for i in session.database_controller.get_list()
        }

        project_names = {
            i.id: i.name
            for i in project_controller.get_list()
        }
        data = []
        for record in query:
            data.append({
                'id': record.object_id,
                'project': project_names[record.project_id],
                'name': record.name.lower(),
                'database': database_names.get(record.database_id, '?'),
                'table': record.table_name,
                'query': record.query_str,
                'last_error': record.last_error,
            })
        return data

from mindsdb_sql_parser.ast import Identifier

from mindsdb_sql_parser import parse_sql, ParsingException

from mindsdb.interfaces.storage import db
from mindsdb.interfaces.database.projects import ProjectController
from mindsdb.utilities.context import context as ctx
from mindsdb.utilities.config import config

from mindsdb.api.executor.controllers.session_controller import SessionController


class TriggersController:
    OBJECT_TYPE = 'trigger'

    def add(self, name, project_name, table, query_str, columns=None):
        name = name.lower()

        if project_name is None:
            project_name = config.get('default_project')
        project_controller = ProjectController()
        project = project_controller.get(name=project_name)

        from mindsdb.api.executor.controllers.session_controller import SessionController
        session = SessionController()

        # check exists
        trigger = self.get_trigger_record(name, project_name)
        if trigger is not None:
            raise Exception(f'Trigger already exists: {name}')

        # check table
        if len(table.parts) < 2:
            raise Exception(f'Database or table not found: {table}')

        table_name = Identifier(parts=table.parts[1:]).to_string()
        db_name = table.parts[0]

        db_integration = session.integration_controller.get(db_name)
        db_handler = session.integration_controller.get_data_handler(db_name)

        if not hasattr(db_handler, 'subscribe'):
            raise Exception(f'Handler {db_integration["engine"]} does''t support subscription')

        df = db_handler.get_tables().data_frame
        column = 'table_name'
        if column not in df.columns:
            column = df.columns[0]
        tables = list(df[column])

        # check only if tables are visible
        if len(tables) > 0 and table_name not in tables:
            raise Exception(f'Table {table_name} not found in {db_name}')

        columns_str = None
        if columns is not None and len(columns) > 0:
            # join to string with delimiter
            columns_str = '|'.join([col.parts[-1] for col in columns])

        # check sql
        try:
            parse_sql(query_str)
        except ParsingException as e:
            raise ParsingException(f'Unable to parse: {query_str}: {e}')

        # create job record
        record = db.Triggers(
            name=name,
            project_id=project.id,

            database_id=db_integration['id'],
            table_name=table_name,
            query_str=query_str,
            columns=columns_str
        )
        db.session.add(record)
        db.session.flush()

        task_record = db.Tasks(
            company_id=ctx.company_id,
            user_class=ctx.user_class,

            object_type=self.OBJECT_TYPE,
            object_id=record.id,
        )
        db.session.add(task_record)
        db.session.commit()

    def delete(self, name, project_name):
        # check exists

        trigger = self.get_trigger_record(name, project_name)
        if trigger is None:
            raise Exception(f"Trigger doesn't exist: {name}")

        task = db.Tasks.query.filter(
            db.Tasks.object_type == self.OBJECT_TYPE,
            db.Tasks.object_id == trigger.id,
            db.Tasks.company_id == ctx.company_id,
        ).first()

        if task is not None:
            db.session.delete(task)

        db.session.delete(trigger)

        db.session.commit()

    def get_trigger_record(self, name, project_name):
        project_controller = ProjectController()
        project = project_controller.get(name=project_name)

        query = db.session.query(
            db.Triggers
        ).join(
            db.Tasks, db.Triggers.id == db.Tasks.object_id
        ).filter(
            db.Triggers.project_id == project.id,
            db.Triggers.name == name,
            db.Tasks.object_type == self.OBJECT_TYPE,
            db.Tasks.company_id == ctx.company_id,
        )
        return query.first()

    def get_list(self, project_name=None):
        session = SessionController()

        query = db.session.query(
            db.Tasks.object_id,
            db.Triggers.project_id,
            db.Triggers.name,
            db.Triggers.database_id,
            db.Triggers.table_name,
            db.Triggers.query_str,
            db.Tasks.last_error,
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

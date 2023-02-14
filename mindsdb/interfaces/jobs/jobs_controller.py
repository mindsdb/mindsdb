import re
import datetime as dt
from dateutil.relativedelta import relativedelta

import sqlalchemy as sa

from mindsdb_sql import parse_sql, ParsingException

from mindsdb.interfaces.storage import db
from mindsdb.interfaces.database.projects import ProjectController
from mindsdb.utilities.context import context as ctx
from mindsdb.utilities import log


def split_sql(sql):
    # split sql by ';' ignoring delimiter in quotes
    pattern = re.compile(r'''((?:[^;"']|"[^"]*"|'[^']*')+)''')
    return pattern.split(sql)[1::2]


def calc_next_date(schedule_str, base_date: dt.datetime):
    schedule_str = schedule_str.lower().strip()

    repeat_prefix = 'every '
    if schedule_str.startswith(repeat_prefix):
        repeat_str = schedule_str[len(repeat_prefix):]
    else:
        # TODO cron format
        raise NotImplementedError(f'Schedule: {schedule_str}')

    items = repeat_str.split()

    if len(items) == 1:
        value = '1'
        period = items[0]
    elif len(items) == 2:
        value, period = items
    else:
        raise Exception(f"Can't parse repeat string: {repeat_str}")

    if not value.isdigit():
        raise Exception(f"Number expected: {value}")
    value = int(value)
    if period in ('minute', 'minutes', 'min'):
        next_date = base_date + dt.timedelta(minutes=value)
    elif period in ('hour', 'hours'):
        next_date = base_date + dt.timedelta(hours=value)
    elif period in ('day', 'days'):
        next_date = base_date + dt.timedelta(days=value)
    elif period in ('week', 'weeks'):
        next_date = base_date + dt.timedelta(days=value * 7)  # 1 week = 7 days
    elif period in ('month', 'months'):
        next_date = base_date + relativedelta(months=value)
    else:
        raise Exception(f"Unknown period: {period}")

    return next_date


class JobsController:
    def add(self, name, project_name, query_str, start_at, end_at, repeat_str):

        if project_name is None:
            project_name = 'mindsdb'
        project_controller = ProjectController()
        project = project_controller.get(name=project_name)

        # check if exists
        record = db.session.query(db.Jobs).filter_by(
            company_id=ctx.company_id,
            name=name,
            project_id=project.id,
            deleted_at=sa.null()
        ).first()
        if record is not None:
            raise Exception(f'Job already exists: {name}')

        if start_at is not None:
            start_at = self._parse_date(start_at)
            if start_at < dt.datetime.now():
                start_at = dt.datetime.now()
        else:
            start_at = dt.datetime.now()

        if end_at is not None:
            end_at = self._parse_date(end_at)

            if end_at < start_at:
                raise Exception(f'Wrong end date {start_at} > {end_at}')

        # check sql = try to parse it
        for sql in split_sql(query_str):
            try:
                # replace template variables with null
                sql = re.sub(r'\{\{[\w\d]+}}', "", sql)

                parse_sql(sql, dialect='mindsdb')
            except ParsingException as e:
                raise ParsingException(f'Unable to parse: {sql}: {e}')

        # plan next run
        next_run_at = start_at

        schedule_str = None
        if repeat_str is not None:
            schedule_str = 'every ' + repeat_str

            # try to calculate schedule string
            calc_next_date(schedule_str, start_at)
        else:
            # no schedule for job end_at is meaningless
            end_at = None

        # create job record
        record = db.Jobs(
            company_id=ctx.company_id,
            name=name,
            project_id=project.id,
            query_str=query_str,
            start_at=start_at,
            end_at=end_at,
            next_run_at=next_run_at,
            schedule_str=schedule_str
        )
        db.session.add(record)
        db.session.commit()

    def _parse_date(self, date_str):

        if date_str.upper() == 'NOW':
            return dt.datetime.now()

        date_formats = ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d']
        date = None
        for date_format in date_formats:
            try:
                date = dt.datetime.strptime(date_str, date_format)
            except ValueError:
                pass
        if date is None:
            raise ValueError(f"Can't parse date: {date_str}")
        return date

    def delete(self, name, project_name):

        project_controller = ProjectController()
        project = project_controller.get(name=project_name)

        # check if exists
        record = db.session.query(db.Jobs).filter_by(
            company_id=ctx.company_id,
            name=name,
            project_id=project.id,
            deleted_at=sa.null()
        ).first()
        if record is None:
            raise Exception(f'Job not exists: {name}')

        self._delete_record(record)
        db.session.commit()

    def _delete_record(self, record):
        record.deleted_at = dt.datetime.now()

    def get_list(self, project_name=None):

        query = db.session.query(db.Jobs).filter_by(
            company_id=ctx.company_id,
            deleted_at=sa.null()
        )

        project_controller = ProjectController()
        if project_name is not None:
            project = project_controller.get(name=project_name)
            query = query.filter_by(project_id=project.id)

        data = []
        project_names = {
            i.id: i.name
            for i in project_controller.get_list()
        }
        for record in query:
            data.append({
                'name': record.name,
                'project': project_names[record.project_id],
                'start_at': record.start_at,
                'end_at': record.end_at,
                'next_run_at': record.next_run_at,
                'schedule_str': record.schedule_str,
                'query': record.query_str,
            })
        return data

    def get_history(self, project_name=None):
        query = db.session.query(db.JobsHistory, db.Jobs).filter_by(
            company_id=ctx.company_id,
        ).outerjoin(db.Jobs, db.Jobs.id == db.JobsHistory.job_id)

        project_controller = ProjectController()
        if project_name is not None:
            project = project_controller.get(name=project_name)
            query = query.filter_by(project_id=project.id)

        data = []
        project_names = {
            i.id: i.name
            for i in project_controller.get_list()
        }
        for record in query:
            data.append({
                'name': record.Jobs.name,
                'project': project_names[record.Jobs.project_id],
                'start_at': record.JobsHistory.start_at,
                'end_at': record.JobsHistory.end_at,
                'error': record.JobsHistory.error,
                'query': record.Jobs.query_str,
            })
        return data


class JobsExecutor:

    def get_next_tasks(self):
        # filter next_run < now
        query = db.session.query(db.Jobs).filter(
            db.Jobs.next_run_at < dt.datetime.now(),
            db.Jobs.deleted_at == sa.null()
        ).order_by(db.Jobs.next_run_at)

        return query.all()

    def update_task_schedule(self, record):
        # calculate next run

        if record.next_run_at > dt.datetime.now():
            # do nothing, it is already planned in future
            return

        if record.schedule_str is None:
            # not need to run it anymore
            self._delete_record(record)
            return

        next_run_at = calc_next_date(record.schedule_str, base_date=record.next_run_at)

        if next_run_at is None:
            # no need to run it
            self._delete_record(record)
        elif record.end_at is not None and next_run_at > record.end_at:
            self._delete_record(record)
        else:
            # plan next run, but not in the past
            if next_run_at < dt.datetime.now():
                next_run_at = dt.datetime.now()
            record.next_run_at = next_run_at

    def _delete_record(self, record):
        record.deleted_at = dt.datetime.now()

    def lock_record(self, record_id):
        # workaround for several concurrent workers on cloud:
        #  create history record before start of task

        record = db.Jobs.query.get(record_id)

        history_record = db.JobsHistory(
            job_id=record.id,
            start_at=record.next_run_at
        )

        db.session.add(history_record)
        db.session.flush()
        return history_record.id

    def execute_task_local(self, record_id, history_id=None):

        record = db.Jobs.query.get(record_id)

        # set up environment

        ctx.set_default()
        ctx.company_id = record.company_id

        if history_id is None:
            history_record = db.JobsHistory(
                job_id=record.id,
                start_at=dt.datetime.now()
            )
        else:
            history_record = db.JobsHistory.query.get(history_id)

        error = ''

        project_controller = ProjectController()
        project = project_controller.get(record.project_id)
        for sql in split_sql(record.query_str):
            try:
                #  fill template variables
                if '{{PREVIOUS_START_DATETIME}}' in sql:
                    # get previous run date
                    history_prev = db.session.query(db.JobsHistory.start_at)\
                        .filter(db.JobsHistory.job_id == record.id,
                                db.JobsHistory.id != history_record.id)\
                        .order_by(db.JobsHistory.id.desc())\
                        .first()
                    if history_prev is None:
                        value = 'null'
                    else:
                        value = history_prev.start_at.strftime("%Y-%m-%d %H:%M:%S")
                    sql = sql.replace('{{PREVIOUS_START_DATETIME}}', value)

                if '{{START_DATE}}' in sql:
                    value = history_record.start_at.strftime("%Y-%m-%d")
                    sql = sql.replace('{{START_DATE}}', value)
                if '{{START_DATETIME}}' in sql:
                    value = history_record.start_at.strftime("%Y-%m-%d %H:%M:%S")
                    sql = sql.replace('{{START_DATETIME}}', value)

                query = parse_sql(sql, dialect='mindsdb')

                from mindsdb.api.mysql.mysql_proxy.controllers.session_controller import SessionController
                from mindsdb.api.mysql.mysql_proxy.executor.executor_commands import ExecuteCommands

                sql_session = SessionController()
                sql_session.database = project.name

                command_executor = ExecuteCommands(sql_session, executor=None)

                ret = command_executor.execute_command(query)
                if ret.error_code is not None:
                    error = ret.error_message
                    break
            except Exception as e:
                error = str(e)
                break

        try:
            self.update_task_schedule(record)
        except Exception as e:
            log.logger.error(f'Error to update schedule: {e}')
            error += f'Error to update schedule: {e}'

            # stop scheduling
            record.next_run_at = None

        if error:
            history_record.error = error
        history_record.end_at = dt.datetime.now()

        db.session.commit()

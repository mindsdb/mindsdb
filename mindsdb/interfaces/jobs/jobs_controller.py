import re
import datetime as dt
from dateutil.relativedelta import relativedelta

import sqlalchemy as sa

from mindsdb_sql import parse_sql, ParsingException
from mindsdb_sql.parser.dialects.mindsdb import CreateJob

from mindsdb.utilities.context import context as ctx
from mindsdb.utilities.exception import EntityNotExistsError, EntityExistsError
from mindsdb.interfaces.storage import db
from mindsdb.interfaces.database.projects import ProjectController
from mindsdb.interfaces.query_context.context_controller import query_context_controller
from mindsdb.utilities import log

logger = log.getLogger(__name__)


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
        delta = dt.timedelta(minutes=value)
    elif period in ('hour', 'hours'):
        delta = dt.timedelta(hours=value)
    elif period in ('day', 'days'):
        delta = dt.timedelta(days=value)
    elif period in ('week', 'weeks'):
        delta = dt.timedelta(days=value * 7)  # 1 week = 7 days
    elif period in ('month', 'months'):
        delta = relativedelta(months=value)
    else:
        raise Exception(f"Unknown period: {period}")

    # period limitation disabled for now
    # config = Config()
    # is_cloud = config.get('cloud', False)
    # if is_cloud and ctx.user_class == 0:
    #     if delta < dt.timedelta(days=1):
    #         raise Exception("Minimal allowed period can't be less than one day")

    next_date = base_date + delta

    return next_date


class JobsController:
    def add(self, name: str, project_name: str, query: CreateJob):

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
            raise EntityExistsError(f'Job already exists: {name}')

        if query.start_str is not None:
            start_at = self._parse_date(query.start_str)
            if start_at < dt.datetime.now():
                start_at = dt.datetime.now()
        else:
            start_at = dt.datetime.now()

        end_at = None
        if query.end_str is not None:
            end_at = self._parse_date(query.end_str)

            if end_at < start_at:
                raise Exception(f'Wrong end date {start_at} > {end_at}')

        # check sql = try to parse it
        for sql in split_sql(query.query_str):
            try:
                # replace template variables with null
                sql = re.sub(r'\{\{[\w\d]+}}', "", sql)

                parse_sql(sql, dialect='mindsdb')
            except ParsingException as e:
                raise ParsingException(f'Unable to parse: {sql}: {e}')

        if query.if_query_str is not None:
            for sql in split_sql(query.if_query_str):
                try:
                    # replace template variables with null
                    sql = re.sub(r'\{\{[\w\d]+}}', "", sql)

                    parse_sql(sql, dialect='mindsdb')
                except ParsingException as e:
                    raise ParsingException(f'Unable to parse: {sql}: {e}')

        # plan next run
        next_run_at = start_at

        schedule_str = None
        if query.repeat_str is not None:
            schedule_str = 'every ' + query.repeat_str

            # try to calculate schedule string
            calc_next_date(schedule_str, start_at)
        else:
            # no schedule for job end_at is meaningless
            end_at = None

        # create job record
        record = db.Jobs(
            company_id=ctx.company_id,
            user_class=ctx.user_class,
            name=name,
            project_id=project.id,
            query_str=query.query_str,
            if_query_str=query.if_query_str,
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
            raise EntityNotExistsError('Job does not exist', name)

        self._delete_record(record)
        db.session.commit()

        # delete context
        query_context_controller.drop_query_context('job', record.id)

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
                'id': record.id,
                'name': record.name,
                'project': project_names[record.project_id],
                'start_at': record.start_at,
                'end_at': record.end_at,
                'next_run_at': record.next_run_at,
                'schedule_str': record.schedule_str,
                'query': record.query_str,
                'if_query': record.if_query_str,
                'variables': query_context_controller.get_context_vars('job', record.id)
            })
        return data


class JobsExecutor:

    def get_next_tasks(self):
        # filter next_run < now
        query = db.session.query(db.Jobs).filter(
            db.Jobs.next_run_at < dt.datetime.now(),
            db.Jobs.deleted_at == sa.null(),
            db.Jobs.active == True,  # noqa
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

        try:

            history_record = db.JobsHistory(
                job_id=record.id,
                start_at=record.next_run_at,
                company_id=record.company_id
            )

            db.session.add(history_record)
            db.session.commit()

            return history_record.id

        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception:
            db.session.rollback()

            # check if it is an old lock
            history_record = db.JobsHistory.query.filter_by(
                job_id=record.id,
                start_at=record.next_run_at,
                company_id=record.company_id
            ).first()
            if history_record.updated_at < dt.datetime.now() - dt.timedelta(seconds=30):
                db.session.delete(history_record)
                db.session.commit()

        return None

    def __fill_variables(self, sql, record, history_record):
        if '{{PREVIOUS_START_DATETIME}}' in sql:
            # get previous run date
            history_prev = db.session.query(db.JobsHistory.start_at) \
                .filter(db.JobsHistory.job_id == record.id,
                        db.JobsHistory.id != history_record.id) \
                .order_by(db.JobsHistory.id.desc()) \
                .first()
            if history_prev is None:
                # start date of the job
                value = record.created_at
            else:
                # fix for twitter: created_at filter must be minimum of 10 seconds prior to the current time
                value = history_prev.start_at - dt.timedelta(seconds=60)
            value = value.strftime("%Y-%m-%d %H:%M:%S")
            sql = sql.replace('{{PREVIOUS_START_DATETIME}}', value)

        if '{{START_DATE}}' in sql:
            value = history_record.start_at.strftime("%Y-%m-%d")
            sql = sql.replace('{{START_DATE}}', value)
        if '{{START_DATETIME}}' in sql:
            value = history_record.start_at.strftime("%Y-%m-%d %H:%M:%S")
            sql = sql.replace('{{START_DATETIME}}', value)
        return sql

    def execute_task_local(self, record_id, history_id=None):

        record = db.Jobs.query.get(record_id)

        # set up environment

        ctx.set_default()
        ctx.company_id = record.company_id
        if record.user_class is not None:
            ctx.user_class = record.user_class

        query_context_controller.set_context('job', record.id)
        if history_id is None:
            history_record = db.JobsHistory(
                job_id=record.id,
                start_at=dt.datetime.now(),
                company_id=record.company_id,
            )
            db.session.add(history_record)
            db.session.flush()
            history_id = history_record.id
            db.session.commit()

        else:
            history_record = db.JobsHistory.query.get(history_id)

        project_controller = ProjectController()
        project = project_controller.get(record.project_id)
        executed_sql = ''

        from mindsdb.api.executor.controllers.session_controller import SessionController
        from mindsdb.api.executor.command_executor import ExecuteCommands

        sql_session = SessionController()
        sql_session.database = project.name
        command_executor = ExecuteCommands(sql_session)

        # job with condition?
        error = ''
        to_execute_query = True
        if record.if_query_str is not None:
            data = None
            for sql in split_sql(record.if_query_str):
                try:
                    #  fill template variables
                    sql = self.__fill_variables(sql, record, history_record)

                    query = parse_sql(sql, dialect='mindsdb')
                    executed_sql += sql + '; '

                    ret = command_executor.execute_command(query)
                    if ret.error_code is not None:
                        error = ret.error_message
                        break

                    data = ret.data
                except Exception as e:
                    logger.error(e)
                    error = str(e)
                    break

            # check error or last result
            if error or data is None or len(data) == 0:
                to_execute_query = False

        if to_execute_query:
            for sql in split_sql(record.query_str):
                try:
                    #  fill template variables
                    sql = self.__fill_variables(sql, record, history_record)

                    query = parse_sql(sql, dialect='mindsdb')
                    executed_sql += sql + '; '

                    ret = command_executor.execute_command(query)
                    if ret.error_code is not None:
                        error = ret.error_message
                        break
                except Exception as e:
                    logger.error(e)
                    error = str(e)
                    break

        try:
            self.update_task_schedule(record)
        except Exception as e:
            db.session.rollback()
            logger.error(f"Error to update schedule: {e}")
            error += f"Error to update schedule: {e}"

            # stop scheduling
            record.next_run_at = None

        history_record = db.JobsHistory.query.get(history_id)

        if error:
            history_record.error = error
        history_record.end_at = dt.datetime.now()
        history_record.query_str = executed_sql

        db.session.commit()

import datetime as dt
from unittest.mock import patch

import pytest

import pandas as pd

from tests.unit.executor_test_base import BaseExecutorDummyML


@pytest.fixture(scope="class")
def scheduler():
    from mindsdb.interfaces.jobs.scheduler import Scheduler
    scheduler_ = Scheduler({})

    yield scheduler_

    scheduler_.stop_thread()


class TestJobs(BaseExecutorDummyML):

    def test_job(self, scheduler):

        df1 = pd.DataFrame([
            {'a': 1, 'c': 1, 'b': dt.datetime(2020, 1, 1)},
            {'a': 2, 'c': 1, 'b': dt.datetime(2020, 1, 2)},
            {'a': 1, 'c': 3, 'b': dt.datetime(2020, 1, 3)},
            {'a': 3, 'c': 2, 'b': dt.datetime(2020, 1, 2)},
        ])
        self.set_data('tbl1', df1)

        self.run_sql('create database proj1')
        # create job
        self.run_sql('create job j1 (select * from models; select * from models)', database='proj1')

        # check jobs table
        ret = self.run_sql('select * from jobs', database='proj1')
        assert len(ret) == 1, "should be 1 job"
        row = ret.iloc[0]
        assert row.NAME == 'j1'
        assert row.START_AT is not None, "start date didn't calc"
        assert row.NEXT_RUN_AT is not None, "next date didn't calc"
        assert row.SCHEDULE_STR is None

        # new project
        self.run_sql('create database proj2')

        # create job with start time and schedule
        self.run_sql('''
            create job proj2.j2 (
                select * from dummy_data.tbl1 where b>'{{PREVIOUS_START_DATETIME}}'
            )
            start now
            every hour
        ''', database='proj1')

        # check jobs table
        ret = self.run_sql('select * from proj2.jobs')
        assert len(ret) == 1, "should be 1 job"
        row = ret.iloc[0]
        assert row.NAME == 'j2'
        assert row.SCHEDULE_STR == 'every hour'

        # check global jobs table
        ret = self.run_sql('select * from information_schema.jobs')
        # all jobs in list
        assert len(ret) == 2
        assert set(ret.NAME.unique()) == {'j1', 'j2'}

        # drop first job
        self.run_sql('drop job proj1.j1')

        # ------------ executing
        scheduler.check_timetable()

        # check query to integration
        job = self.db.Jobs.query.filter(self.db.Jobs.name == 'j2').first()

        # check jobs table
        ret = self.run_sql('select * from jobs', database='proj2')
        # next run is about 60 minutes from previous
        minutes = (ret.NEXT_RUN_AT - ret.START_AT)[0].seconds / 60
        assert minutes > 58 and minutes < 62

        # check history table
        ret = self.run_sql('select * from log.jobs_history', database='proj2')
        # proj2.j2 was run one time
        assert len(ret) == 1
        assert ret.project[0] == 'proj2' and ret.name[0] == 'j2'

        # run once again
        scheduler.check_timetable()

        # job wasn't executed
        ret = self.run_sql('select * from log.jobs_history', database='proj2')
        assert len(ret) == 1

        # shift 'next run' and run once again
        job = self.db.Jobs.query.filter(self.db.Jobs.name == 'j2').first()
        job.next_run_at = job.start_at - dt.timedelta(seconds=1)  # different time because there is unique key
        self.db.session.commit()

        scheduler.check_timetable()

        ret = self.run_sql('select * from log.jobs_history', database='proj2')
        assert len(ret) == 2  # was executed

        # check global history table
        # ret = self.run_sql('select * from information_schema.jobs_history', database='proj2')
        # assert len(ret) == 2
        # assert sorted([x.upper() for x in list(ret.columns)]) == sorted([x.upper() for x in JobsHistoryTable.columns])

        # there is no 'jobs_history' table in project
        with pytest.raises(Exception):
            self.run_sql('select * from jobs_history', database='proj2')

        with pytest.raises(Exception):
            self.run_sql('select company_id from log.jobs_history', database='proj2')

    def test_inactive_job(self, scheduler):
        # create job
        self.run_sql('create job j1 (select * from models)')

        # check jobs table
        ret = self.run_sql('select * from jobs')
        assert len(ret) == 1, "should be 1 job"

        # deactivate
        job = self.db.Jobs.query.filter(self.db.Jobs.name == 'j1').first()
        job.active = False
        self.db.session.commit()

        # run scheduler
        scheduler.check_timetable()

        ret = self.run_sql('select * from log.jobs_history')
        # no history
        assert len(ret) == 0

    def test_conditional_job(self, scheduler):
        df = pd.DataFrame([
            {'a': 1, 'b': '2'},
        ])
        self.save_file('tasks', df)

        # create job
        job_str = '''
            create job j1 (
                CREATE model pred
                PREDICT p
                using engine='dummy_ml',
                join_learn_process=true
            )
            if (
                select * from files.tasks where a={var}
            )
        '''

        self.run_sql(job_str.format(var=2))

        # check jobs table
        ret = self.run_sql('select * from jobs')
        assert len(ret) == 1, "should be 1 job"

        # run scheduler
        scheduler.check_timetable()

        # check no models created
        ret = self.run_sql('select * from models where name="pred"')
        assert len(ret) == 0

        # --- attempt2 ---

        self.run_sql(job_str.format(var=1))

        # check jobs table, still one job - previous was one time job
        ret = self.run_sql('select * from jobs')
        assert len(ret) == 1, "should be 1 job"

        # run scheduler
        scheduler.check_timetable()

        # check 1 model
        ret = self.run_sql('select * from models where name="pred"')
        assert len(ret) == 1

    @patch('mindsdb.integrations.handlers.postgres_handler.Handler')
    def test_last_in_job(self, data_handler, scheduler):
        df = pd.DataFrame([
            {'a': 1, 'b': 'a'},
            {'a': 2, 'b': 'b'},
        ])
        self.set_handler(data_handler, name='pg', tables={'tasks': df})
        self.save_file('tasks', df)

        # -- create model --
        self.run_sql(
            '''
                CREATE model task_model
                from files (select * from tasks)
                PREDICT a
                using engine='dummy_ml'
            '''
        )

        # create job to update table
        self.run_sql('''
          create job j1  (
            create table files.t1  (
                SELECT m.*
                   FROM pg.tasks as t
                   JOIN task_model as m
                   where t.a > last and t.b='b'
            )
          )
          start now
          every hour
        ''')

        scheduler.check_timetable()

        # table size didn't change
        calls = data_handler().query.call_args_list
        sql = calls[0][0][0].to_string()
        # getting current last value
        assert 'ORDER BY a DESC LIMIT 1' in sql

        # insert new record to source db

        df.loc[len(df.index)] = [6, 'a']

        data_handler.reset_mock()
        # shift 'next run' and run once again
        job = self.db.Jobs.query.filter(self.db.Jobs.name == 'j1').first()
        job.next_run_at = job.start_at - dt.timedelta(seconds=1)  # different time because there is unique key
        self.db.session.commit()

        scheduler.check_timetable()

        calls = data_handler().query.call_args_list

        assert len(calls) == 1
        sql = calls[0][0][0].to_string()
        # getting next value, greater than max previous
        assert 'a > 2' in sql
        assert "b = 'b'" in sql

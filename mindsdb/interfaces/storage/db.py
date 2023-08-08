import os
import json
import datetime
from typing import Dict

import numpy as np
from sqlalchemy import create_engine, types, UniqueConstraint
from sqlalchemy.orm import scoped_session, sessionmaker, declarative_base
from sqlalchemy import Column, Integer, String, DateTime, Boolean, Index, text
from sqlalchemy.sql.schema import ForeignKey
from sqlalchemy import JSON
from sqlalchemy.exc import OperationalError


class Base:
    __allow_unmapped__ = True


Base = declarative_base(cls=Base)

session, engine = None, None


def init(connection_str: str = None):
    global Base, session, engine
    if connection_str is None:
        connection_str = os.environ['MINDSDB_DB_CON']
    base_args = {
        'pool_size': 30,
        'max_overflow': 200
    }
    engine = create_engine(connection_str, echo=False, **base_args)
    session = scoped_session(sessionmaker(bind=engine, autoflush=True))
    Base.query = session.query_property()


def serializable_insert(record: Base, try_count: int = 100):
    """ Do serializeble insert. If fail - repeat it {try_count} times.

        Args:
            record (Base): sqlalchey record to insert
            try_count (int): count of tryes to insert record
    """
    commited = False
    while not commited:
        session.connection(
            execution_options={'isolation_level': 'SERIALIZABLE'}
        )
        if engine.name == 'postgresql':
            session.execute(text('LOCK TABLE PREDICTOR IN EXCLUSIVE MODE'))
        session.add(record)
        try:
            session.commit()
        except OperationalError:
            # catch 'SerializationFailure' (it should be in str(e), but it may depend on engine)
            session.rollback()
            try_count += -1
            if try_count == 0:
                raise
        else:
            commited = True


# Source: https://stackoverflow.com/questions/26646362/numpy-array-is-not-json-serializable
class NumpyEncoder(json.JSONEncoder):
    """ Special json encoder for numpy types """
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)


class Array(types.TypeDecorator):
    ''' Float Type that replaces commas with  dots on input '''
    impl = types.String

    def process_bind_param(self, value, dialect):  # insert
        if isinstance(value, str):
            return value
        elif value is None:
            return value
        else:
            return ',|,|,'.join(value)

    def process_result_value(self, value, dialect):  # select
        return value.split(',|,|,') if value is not None else None


class Json(types.TypeDecorator):
    ''' Float Type that replaces commas with  dots on input '''
    impl = types.String

    def process_bind_param(self, value, dialect):  # insert
        return json.dumps(value, cls=NumpyEncoder) if value is not None else None

    def process_result_value(self, value, dialect):  # select
        if isinstance(value, dict):
            return value
        return json.loads(value) if value is not None else None


class Semaphor(Base):
    __tablename__ = 'semaphor'

    id = Column(Integer, primary_key=True)
    updated_at = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)
    created_at = Column(DateTime, default=datetime.datetime.now)
    entity_type = Column('entity_type', String)
    entity_id = Column('entity_id', Integer)
    action = Column(String)
    company_id = Column(Integer)
    __table_args__ = (
        UniqueConstraint('entity_type', 'entity_id', name='uniq_const'),
    )


class PREDICTOR_STATUS:
    __slots__ = ()
    COMPLETE = 'complete'
    TRAINING = 'training'
    FINETUNING = 'finetuning'
    GENERATING = 'generating'
    ERROR = 'error'
    VALIDATION = 'validation'
    DELETED = 'deleted'  # TODO remove it?


PREDICTOR_STATUS = PREDICTOR_STATUS()


class Predictor(Base):
    __tablename__ = 'predictor'

    id = Column(Integer, primary_key=True)
    updated_at = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)
    created_at = Column(DateTime, default=datetime.datetime.now)
    deleted_at = Column(DateTime)
    name = Column(String)
    data = Column(Json)  # A JSON -- should be everything returned by `get_model_data`, I think
    to_predict = Column(Array)
    company_id = Column(Integer)
    mindsdb_version = Column(String)
    native_version = Column(String)
    integration_id = Column(ForeignKey('integration.id', name='fk_integration_id'))
    data_integration_ref = Column(Json)
    fetch_data_query = Column(String)
    is_custom = Column(Boolean)
    learn_args = Column(Json)
    update_status = Column(String, default='up_to_date')
    status = Column(String)
    active = Column(Boolean, default=True)
    training_data_columns_count = Column(Integer)
    training_data_rows_count = Column(Integer)
    training_start_at = Column(DateTime)
    training_stop_at = Column(DateTime)
    label = Column(String, nullable=True)
    version = Column(Integer, default=1)

    code = Column(String, nullable=True)
    lightwood_version = Column(String, nullable=True)
    dtype_dict = Column(Json, nullable=True)
    project_id = Column(Integer, ForeignKey('project.id', name='fk_project_id'), nullable=False)
    training_phase_current = Column(Integer)
    training_phase_total = Column(Integer)
    training_phase_name = Column(String)

    @staticmethod
    def get_name_and_version(full_name):
        name_no_version = full_name
        version = None
        parts = full_name.split('.')
        if len(parts) > 1 and parts[-1].isdigit():
            version = int(parts[-1])
            name_no_version = '.'.join(parts[:-1])
        return name_no_version, version


class Project(Base):
    __tablename__ = 'project'

    id = Column(Integer, primary_key=True)
    created_at = Column(DateTime, default=datetime.datetime.now)
    updated_at = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)
    deleted_at = Column(DateTime)
    name = Column(String, nullable=False)
    company_id = Column(Integer)
    __table_args__ = (
        UniqueConstraint('name', 'company_id', name='unique_project_name_company_id'),
    )


class Log(Base):
    __tablename__ = 'log'

    id = Column(Integer, primary_key=True)
    created_at = Column(DateTime, default=datetime.datetime.now)
    log_type = Column(String)  # log, info, warning, traceback etc
    source = Column(String)  # file + line
    company_id = Column(Integer)
    payload = Column(String)
    created_at_index = Index("some_index", "created_at_index")


class Integration(Base):
    __tablename__ = 'integration'
    id = Column(Integer, primary_key=True)
    updated_at = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)
    created_at = Column(DateTime, default=datetime.datetime.now)
    name = Column(String, nullable=False)
    engine = Column(String, nullable=False)
    data = Column(Json)
    company_id = Column(Integer)
    __table_args__ = (
        UniqueConstraint('name', 'company_id', name='unique_integration_name_company_id'),
    )


class File(Base):
    __tablename__ = 'file'
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    company_id = Column(Integer)
    source_file_path = Column(String, nullable=False)
    file_path = Column(String, nullable=False)
    row_count = Column(Integer, nullable=False)
    columns = Column(Json, nullable=False)
    created_at = Column(DateTime, default=datetime.datetime.now)
    updated_at = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)
    __table_args__ = (
        UniqueConstraint('name', 'company_id', name='unique_file_name_company_id'),
    )


class View(Base):
    __tablename__ = 'view'
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    company_id = Column(Integer)
    query = Column(String, nullable=False)
    project_id = Column(Integer, ForeignKey('project.id', name='fk_project_id'), nullable=False)
    __table_args__ = (
        UniqueConstraint('name', 'company_id', name='unique_view_name_company_id'),
    )


class JsonStorage(Base):
    __tablename__ = 'json_storage'
    id = Column(Integer, primary_key=True)
    resource_group = Column(String)
    resource_id = Column(Integer)
    name = Column(String)
    content = Column(JSON)
    company_id = Column(Integer)


class Jobs(Base):
    __tablename__ = 'jobs'
    id = Column(Integer, primary_key=True)
    company_id = Column(Integer)
    user_class = Column(Integer, nullable=True)

    name = Column(String, nullable=False)
    project_id = Column(Integer, nullable=False)
    query_str = Column(String, nullable=False)
    start_at = Column(DateTime, default=datetime.datetime.now)
    end_at = Column(DateTime)
    next_run_at = Column(DateTime)
    schedule_str = Column(String)

    deleted_at = Column(DateTime)
    updated_at = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)
    created_at = Column(DateTime, default=datetime.datetime.now)


class JobsHistory(Base):
    __tablename__ = 'jobs_history'
    id = Column(Integer, primary_key=True)
    company_id = Column(Integer)

    job_id = Column(Integer)

    query_str = Column(String)
    start_at = Column(DateTime)
    end_at = Column(DateTime)

    error = Column(String)
    created_at = Column(DateTime, default=datetime.datetime.now)
    updated_at = Column(DateTime, default=datetime.datetime.now)

    __table_args__ = (
        UniqueConstraint('job_id', 'start_at', name='uniq_job_history_job_id_start'),
    )


class ChatBots(Base):
    __tablename__ = 'chat_bots'
    id = Column(Integer, primary_key=True)

    name = Column(String, nullable=False)
    project_id = Column(Integer, nullable=False)

    model_name = Column(String, nullable=False)
    database_id = Column(Integer)
    params = Column(JSON)

    updated_at = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)
    created_at = Column(DateTime, default=datetime.datetime.now)

    def as_dict(self) -> Dict:
        return {
            'id': self.id,
            'name': self.name,
            'project_id': self.project_id,
            'model_name': self.model_name,
            'params': self.params,
            'created_at': self.created_at,
            'database_id': self.database_id,
        }


class ChatBotsHistory(Base):
    __tablename__ = 'chat_bots_history'
    id = Column(Integer, primary_key=True)
    chat_bot_id = Column(Integer)
    type = Column(String)  # TODO replace to enum
    text = Column(String)
    user = Column(String)
    destination = Column(String)
    sent_at = Column(DateTime, default=datetime.datetime.now)
    error = Column(String)


class Triggers(Base):
    __tablename__ = 'triggers'
    id = Column(Integer, primary_key=True)

    name = Column(String, nullable=False)
    project_id = Column(Integer, nullable=False)

    database_id = Column(Integer, nullable=False)
    table_name = Column(String, nullable=False)
    query_str = Column(String, nullable=False)
    columns = Column(String)  # list of columns separated by delimiter

    updated_at = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)
    created_at = Column(DateTime, default=datetime.datetime.now)


class Tasks(Base):
    __tablename__ = 'tasks'
    id = Column(Integer, primary_key=True)
    company_id = Column(Integer)
    user_class = Column(Integer, nullable=True)

    # trigger, chatbot
    object_type = Column(String, nullable=False)
    object_id = Column(Integer, nullable=False)

    last_error = Column(String)
    active = Column(Boolean, default=True)
    reload = Column(Boolean, default=False)

    # for running in concurrent processes
    run_by = Column(String)
    alive_time = Column(DateTime(timezone=True))

    updated_at = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)
    created_at = Column(DateTime, default=datetime.datetime.now)

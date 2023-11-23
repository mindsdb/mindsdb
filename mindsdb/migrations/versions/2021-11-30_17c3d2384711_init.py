import datetime

from alembic.autogenerate import produce_migrations, render, api
from alembic import context
from sqlalchemy import UniqueConstraint
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, Boolean, Index

# required for code execution
from alembic import op  # noqa
import sqlalchemy as sa  # noqa

import mindsdb.interfaces.storage.db    # noqa
from mindsdb.interfaces.storage.db import Json, Array
from mindsdb.utilities import log

logger = log.getLogger(__name__)

# revision identifiers, used by Alembic.
revision = '17c3d2384711'
down_revision = None
branch_labels = None
depends_on = None

# ========================================== current database state ========================================


class Base:
    __allow_unmapped__ = True


Base = declarative_base(cls=Base)

# Source: https://stackoverflow.com/questions/26646362/numpy-array-is-not-json-serializable


class Semaphor(Base):
    __tablename__ = 'semaphor'

    id = Column(Integer, primary_key=True)
    updated_at = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)
    created_at = Column(DateTime, default=datetime.datetime.now)
    entity_type = Column('entity_type', String)
    entity_id = Column('entity_id', Integer)
    action = Column(String)
    company_id = Column(Integer)
    uniq_const = UniqueConstraint('entity_type', 'entity_id')


class Datasource(Base):
    __tablename__ = 'datasource'

    id = Column(Integer, primary_key=True)
    updated_at = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)
    created_at = Column(DateTime, default=datetime.datetime.now)
    name = Column(String)
    data = Column(String)  # Including, e.g. the query used to create it and even the connection info when there's no integration associated with it -- A JSON
    creation_info = Column(String)
    analysis = Column(String)  # A JSON
    company_id = Column(Integer)
    mindsdb_version = Column(String)
    datasources_version = Column(String)
    integration_id = Column(Integer)


class Predictor(Base):
    __tablename__ = 'predictor'

    id = Column(Integer, primary_key=True)
    updated_at = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)
    created_at = Column(DateTime, default=datetime.datetime.now)
    name = Column(String)
    data = Column(Json)  # A JSON -- should be everything returned by `get_model_data`, I think
    to_predict = Column(Array)
    company_id = Column(Integer)
    mindsdb_version = Column(String)
    native_version = Column(String)
    datasource_id = Column(Integer)
    is_custom = Column(Boolean)     # to del
    learn_args = Column(Json)
    update_status = Column(String, default='up_to_date')

    json_ai = Column(Json, nullable=True)
    code = Column(String, nullable=True)
    lightwood_version = Column(String, nullable=True)
    dtype_dict = Column(Json, nullable=True)


class AITable(Base):
    __tablename__ = 'ai_table'
    id = Column(Integer, primary_key=True)
    updated_at = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)
    created_at = Column(DateTime, default=datetime.datetime.now)
    name = Column(String)
    integration_name = Column(String)
    integration_query = Column(String)
    query_fields = Column(Json)
    predictor_name = Column(String)
    predictor_columns = Column(Json)
    company_id = Column(Integer)


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
    data = Column(Json)
    company_id = Column(Integer)


class Stream(Base):
    __tablename__ = 'stream'
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    stream_in = Column(String, nullable=False)
    stream_out = Column(String, nullable=False)
    anomaly_stream = Column(String)
    integration = Column(String)
    predictor = Column(String, nullable=False)
    company_id = Column(Integer)
    updated_at = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)
    created_at = Column(DateTime, default=datetime.datetime.now)
    type = Column(String, default='unknown')
    connection_info = Column(Json, default={})
    learning_params = Column(Json, default={})
    learning_threshold = Column(Integer, default=0)


# ====================================================================================================


def upgrade():
    '''
       First migration.
       Generates a migration script by difference between model and database and executes it
    '''

    target_metadata = Base.metadata

    mc = context.get_context()

    migration_script = produce_migrations(mc, target_metadata)

    autogen_context = api.AutogenContext(
        mc, autogenerate=True
    )

    # Seems to be the only way to apply changes to the database
    template_args = {}
    render._render_python_into_templatevars(
        autogen_context, migration_script, template_args
    )

    code = template_args['upgrades']
    code = code.replace('\n    ', '\n')
    logger.info('\nPerforming database changes:')
    logger.info(code)
    exec(code)


def downgrade():

    # We don't know state to downgrade
    raise NotImplementedError()

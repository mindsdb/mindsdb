import os
import json

import numpy as np
from sqlalchemy import create_engine, orm, types
from sqlalchemy.orm import scoped_session, sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, MetaData, Index
from sqlalchemy.schema import ForeignKey
import datetime


engine = create_engine(os.environ['MINDSDB_DB_CON'], echo=False) # + '?check_same_thread=False'
Base = declarative_base()
session = scoped_session(sessionmaker(bind=engine, autoflush=True))
Base.query = session.query_property()
entitiy_version = 1


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
        return json.loads(value) if value is not None else None

class Semaphor(Base):
    __tablename__ = 'semaphor'

    id = Column(Integer, primary_key=True)
    updated_at = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)
    created_at = Column(DateTime, default=datetime.datetime.now)
    entity_type = Column(String)
    entity_id = Column(String)

class Configuration(Base):
    __tablename__ = 'configuration'

    id = Column(Integer, primary_key=True)
    updated_at = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)
    created_at = Column(DateTime, default=datetime.datetime.now)
    data = Column(String) # A JSON
    company_id = Column(Integer, unique=True)

class Datasource(Base):
    __tablename__ = 'datasource'

    id = Column(Integer, primary_key=True)
    updated_at = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)
    created_at = Column(DateTime, default=datetime.datetime.now)
    name = Column(String)
    data = Column(String) # Including, e.g. the query used to create it and even the connection info when there's no integration associated with it -- A JSON
    creation_info = Column(String)
    analysis = Column(String)  # A JSON
    company_id = Column(Integer)
    version = Column(Integer, default=entitiy_version)
    integration_id = Column(Integer)


class Predictor(Base):
    __tablename__ = 'predictor'

    id = Column(Integer, primary_key=True)
    updated_at = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)
    created_at = Column(DateTime, default=datetime.datetime.now)
    name = Column(String)
    data = Column(Json) # A JSON -- should be everything returned by `get_model_data`, I think
    to_predict = Column(Array)
    company_id = Column(Integer)
    version = Column(Integer, default=entitiy_version) # mindsdb_native version, can be used in the future for BC
    datasource_id = Column(Integer, ForeignKey('datasource.id'))
    is_custom = Column(Boolean)

class Log(Base):
    __tablename__ = 'log'

    id = Column(Integer, primary_key=True)
    created_at = Column(DateTime, default=datetime.datetime.now)
    log_type = Column(String) # log, info, warning, traceback etc
    source = Column(String) # file + line
    company_id = Column(Integer)
    payload = Column(String)
    created_at_index = Index("some_index", "created_at_index")


Base.metadata.create_all(engine)
orm.configure_mappers()

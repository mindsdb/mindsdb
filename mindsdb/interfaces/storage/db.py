import os
from sqlalchemy import create_engine, orm
from sqlalchemy.orm import scoped_session, sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, MetaData
from sqlalchemy.schema import ForeignKey
import datetime

if os.environ['MINDSDB_DATABASE_TYPE'] == 'sqlite':
    engine = create_engine('sqlite:///' + os.environ['MINDSDB_SQLITE_PATH'], echo=False)
elif os.environ['MINDSDB_DATABASE_TYPE'] == 'mariadb':
    raise Exception('Mariadb not supported !')

Base = declarative_base()
session = scoped_session(sessionmaker(bind=engine,autoflush=True))
Base.query = session.query_property()
entitiy_version = 1

class Semaphor(Base):
    __tablename__ = 'semaphor'

    id = Column(Integer, primary_key=True)
    modified_at = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)
    created_at = Column(DateTime, default=datetime.datetime.now)
    entity_type = Column(String)
    entity_id = Column(String)

class Configuration(Base):
    __tablename__ = 'configuration'

    id = Column(Integer, primary_key=True)
    modified_at = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)
    created_at = Column(DateTime, default=datetime.datetime.now)
    data = Column(String) # A JSON
    company_id = Column(Integer, unique=True)

class Datasource(Base):
    __tablename__ = 'datasource'

    id = Column(Integer, primary_key=True)
    modified_at = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)
    created_at = Column(DateTime, default=datetime.datetime.now)
    name = Column(String)
    data = Column(String) # Including, e.g. the query used to create it and even the connection info when there's no integration associated with it -- A JSON
    analysis = Column(String)  # A JSON
    company_id = Column(Integer)
    version = Column(Integer, default=entitiy_version)
    integration_id = Column(Integer)


class Predictor(Base):
    __tablename__ = 'predictor'

    id = Column(Integer, primary_key=True)
    modified_at = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)
    created_at = Column(DateTime, default=datetime.datetime.now)
    name = Column(String)
    data = Column(String) # A JSON
    native_version = Column(String)
    to_predict = Column(String)
    status = Column(String)
    company_id = Column(Integer)
    version = Column(Integer, default=entitiy_version)
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


Base.metadata.create_all(engine)
orm.configure_mappers()

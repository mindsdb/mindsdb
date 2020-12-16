from sqlalchemy import create_engine, orm
from sqlalchemy.orm import scoped_session, sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, MetaData
from sqlalchemy.schema import ForeignKey
import datetime


engine = create_engine('sqlite:///test.db', echo=True)
Base = declarative_base()
session = scoped_session(sessionmaker(bind=engine,autoflush=True))
Base.query = session.query_property()

class Semaphor(Base):
    __tablename__ = 'semaphor'

    id = Column(Integer, primary_key=True)
    modified_at = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)
    create_at = Column(DateTime, default=datetime.datetime.now)
    entity_type = Column(String)
    entity_id = Column(String)

class Registration(Base):
    __tablename__ = 'registration'

    id = Column(Integer, primary_key=True)
    modified_at = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)
    create_at = Column(DateTime, default=datetime.datetime.now)
    integration_id = Column(Integer, ForeignKey('integration.id'))
    predictor_id = Column(Integer, ForeignKey('predictor.id'))
    company_id = Column(Integer)

class Configuration(Base):
    __tablename__ = 'configuration'

    id = Column(Integer, primary_key=True)
    modified_at = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)
    create_at = Column(DateTime, default=datetime.datetime.now)
    data = Column(String) # A JSON
    company_id = Column(Integer, unique=True)

class Integration(Base):
    __tablename__ = 'integration'

    id = Column(Integer, primary_key=True)
    modified_at = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)
    create_at = Column(DateTime, default=datetime.datetime.now)
    name = Column(String)
    tpye = Column(String)
    attributes = Column(String) # A JSON
    status = Column(String)
    publishable = Column(Boolean)
    company_id = Column(Integer)

class Datasource(Base):
    __tablename__ = 'datasource'

    id = Column(Integer, primary_key=True)
    modified_at = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)
    create_at = Column(DateTime, default=datetime.datetime.now)
    name = Column(String)
    data = Column(String) # Including, e.g. the query used to create it and even the connection info when there's no integration associated with it -- A JSON
    analysis = Column(String)  # A JSON
    company_id = Column(Integer)
    storage_path = Column(String)
    integration_id = Column(Integer, ForeignKey('integration.id'))


class Predictor(Base):
    __tablename__ = 'predictor'

    id = Column(Integer, primary_key=True)
    modified_at = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)
    create_at = Column(DateTime, default=datetime.datetime.now)
    name = Column(String)
    data = Column(String) # A JSON
    native_version = Column(String)
    to_predict = Column(String)
    status = Column(String)
    company_id = Column(Integer)
    storage_path = Column(String)
    datasource_id = Column(Integer, ForeignKey('datasource.id'))

Base.metadata.create_all(engine)
orm.configure_mappers()

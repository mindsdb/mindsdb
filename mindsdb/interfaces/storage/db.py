import datetime
import json
import os
from typing import Dict, List

import numpy as np
from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    DateTime,
    Index,
    Integer,
    String,
    Table,
    UniqueConstraint,
    create_engine,
    text,
    types
)
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import (
    Mapped,
    declarative_base,
    relationship,
    scoped_session,
    sessionmaker,
)
from sqlalchemy.sql.schema import ForeignKey

from mindsdb.utilities.json_encoder import CustomJSONEncoder


class Base:
    __allow_unmapped__ = True


Base = declarative_base(cls=Base)

session, engine = None, None


def init(connection_str: str = None):
    global Base, session, engine
    if connection_str is None:
        connection_str = os.environ["MINDSDB_DB_CON"]
    base_args = {
        "pool_size": 30,
        "max_overflow": 200,
        "json_serializer": CustomJSONEncoder().encode,
    }
    engine = create_engine(connection_str, echo=False, **base_args)
    session = scoped_session(sessionmaker(bind=engine, autoflush=True))
    Base.query = session.query_property()


def serializable_insert(record: Base, try_count: int = 100):
    """Do serializeble insert. If fail - repeat it {try_count} times.

    Args:
        record (Base): sqlalchey record to insert
        try_count (int): count of tryes to insert record
    """
    commited = False
    while not commited:
        session.connection(execution_options={"isolation_level": "SERIALIZABLE"})
        if engine.name == "postgresql":
            session.execute(text("LOCK TABLE PREDICTOR IN EXCLUSIVE MODE"))
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
    """Special json encoder for numpy types"""

    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)


class Array(types.TypeDecorator):
    """Float Type that replaces commas with  dots on input"""

    impl = types.String

    def process_bind_param(self, value, dialect):  # insert
        if isinstance(value, str):
            return value
        elif value is None:
            return value
        else:
            return ",|,|,".join(value)

    def process_result_value(self, value, dialect):  # select
        return value.split(",|,|,") if value is not None else None


class Json(types.TypeDecorator):
    """Float Type that replaces commas with  dots on input"""

    impl = types.String

    def process_bind_param(self, value, dialect):  # insert
        return json.dumps(value, cls=NumpyEncoder) if value is not None else None

    def process_result_value(self, value, dialect):  # select
        if isinstance(value, dict):
            return value
        return json.loads(value) if value is not None else None


class PREDICTOR_STATUS:
    __slots__ = ()
    COMPLETE = "complete"
    TRAINING = "training"
    FINETUNING = "finetuning"
    GENERATING = "generating"
    ERROR = "error"
    VALIDATION = "validation"
    DELETED = "deleted"  # TODO remove it?


PREDICTOR_STATUS = PREDICTOR_STATUS()


class Predictor(Base):
    __tablename__ = "predictor"

    id = Column(Integer, primary_key=True)
    updated_at = Column(
        DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now
    )
    created_at = Column(DateTime, default=datetime.datetime.now)
    deleted_at = Column(DateTime)
    name = Column(String)
    data = Column(
        Json
    )  # A JSON -- should be everything returned by `get_model_data`, I think
    to_predict = Column(Array)
    company_id = Column(Integer)
    mindsdb_version = Column(String)
    native_version = Column(String)
    integration_id = Column(ForeignKey("integration.id", name="fk_integration_id"))
    data_integration_ref = Column(Json)
    fetch_data_query = Column(String)
    is_custom = Column(Boolean)
    learn_args = Column(Json)
    update_status = Column(String, default="up_to_date")
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
    project_id = Column(
        Integer, ForeignKey("project.id", name="fk_project_id"), nullable=False
    )
    training_phase_current = Column(Integer)
    training_phase_total = Column(Integer)
    training_phase_name = Column(String)
    hostname = Column(String)

    @staticmethod
    def get_name_and_version(full_name):
        name_no_version = full_name
        version = None
        parts = full_name.split(".")
        if len(parts) > 1 and parts[-1].isdigit():
            version = int(parts[-1])
            name_no_version = ".".join(parts[:-1])
        return name_no_version, version


Index(
    "predictor_index",
    Predictor.company_id,
    Predictor.name,
    Predictor.version,
    Predictor.active,
    Predictor.deleted_at,  # would be good to have here nullsfirst(Predictor.deleted_at)
    unique=True
)


class Project(Base):
    __tablename__ = "project"

    id = Column(Integer, primary_key=True)
    created_at = Column(DateTime, default=datetime.datetime.now)
    updated_at = Column(
        DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now
    )
    deleted_at = Column(DateTime)
    name = Column(String, nullable=False)
    company_id = Column(Integer)
    __table_args__ = (
        UniqueConstraint("name", "company_id", name="unique_project_name_company_id"),
    )


class Log(Base):
    __tablename__ = "log"

    id = Column(Integer, primary_key=True)
    created_at = Column(DateTime, default=datetime.datetime.now)
    log_type = Column(String)  # log, info, warning, traceback etc
    source = Column(String)  # file + line
    company_id = Column(Integer)
    payload = Column(String)
    created_at_index = Index("some_index", "created_at_index")


class Integration(Base):
    __tablename__ = "integration"
    id = Column(Integer, primary_key=True)
    updated_at = Column(
        DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now
    )
    created_at = Column(DateTime, default=datetime.datetime.now)
    name = Column(String, nullable=False)
    engine = Column(String, nullable=False)
    data = Column(Json)
    company_id = Column(Integer)
    __table_args__ = (
        UniqueConstraint(
            "name", "company_id", name="unique_integration_name_company_id"
        ),
    )


class File(Base):
    __tablename__ = "file"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    company_id = Column(Integer)
    source_file_path = Column(String, nullable=False)
    file_path = Column(String, nullable=False)
    row_count = Column(Integer, nullable=False)
    columns = Column(Json, nullable=False)
    created_at = Column(DateTime, default=datetime.datetime.now)
    updated_at = Column(
        DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now
    )
    __table_args__ = (
        UniqueConstraint("name", "company_id", name="unique_file_name_company_id"),
    )


class View(Base):
    __tablename__ = "view"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    company_id = Column(Integer)
    query = Column(String, nullable=False)
    project_id = Column(
        Integer, ForeignKey("project.id", name="fk_project_id"), nullable=False
    )
    __table_args__ = (
        UniqueConstraint("name", "company_id", name="unique_view_name_company_id"),
    )


class JsonStorage(Base):
    __tablename__ = "json_storage"
    id = Column(Integer, primary_key=True)
    resource_group = Column(String)
    resource_id = Column(Integer)
    name = Column(String)
    content = Column(JSON)
    company_id = Column(Integer)


class Jobs(Base):
    __tablename__ = "jobs"
    id = Column(Integer, primary_key=True)
    company_id = Column(Integer)
    user_class = Column(Integer, nullable=True)
    active = Column(Boolean, default=True)

    name = Column(String, nullable=False)
    project_id = Column(Integer, nullable=False)
    query_str = Column(String, nullable=False)
    if_query_str = Column(String, nullable=True)
    start_at = Column(DateTime, default=datetime.datetime.now)
    end_at = Column(DateTime)
    next_run_at = Column(DateTime)
    schedule_str = Column(String)

    deleted_at = Column(DateTime)
    updated_at = Column(
        DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now
    )
    created_at = Column(DateTime, default=datetime.datetime.now)


class JobsHistory(Base):
    __tablename__ = "jobs_history"
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
        UniqueConstraint("job_id", "start_at", name="uniq_job_history_job_id_start"),
    )


class ChatBots(Base):
    __tablename__ = "chat_bots"
    id = Column(Integer, primary_key=True)

    name = Column(String, nullable=False)
    project_id = Column(Integer, nullable=False)
    agent_id = Column(ForeignKey("agents.id", name="fk_agent_id"))

    # To be removed when existing chatbots are backfilled with newly created Agents.
    model_name = Column(String)
    database_id = Column(Integer)
    params = Column(JSON)

    updated_at = Column(
        DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now
    )
    created_at = Column(DateTime, default=datetime.datetime.now)
    webhook_token = Column(String)

    def as_dict(self) -> Dict:
        return {
            "id": self.id,
            "name": self.name,
            "project_id": self.project_id,
            "agent_id": self.agent_id,
            "model_name": self.model_name,
            "params": self.params,
            "webhook_token": self.webhook_token,
            "created_at": self.created_at,
            "database_id": self.database_id,
        }


class ChatBotsHistory(Base):
    __tablename__ = "chat_bots_history"
    id = Column(Integer, primary_key=True)
    chat_bot_id = Column(Integer, nullable=False)
    type = Column(String)  # TODO replace to enum
    text = Column(String)
    user = Column(String)
    destination = Column(String)
    sent_at = Column(DateTime, default=datetime.datetime.now)
    error = Column(String)


class Triggers(Base):
    __tablename__ = "triggers"
    id = Column(Integer, primary_key=True)

    name = Column(String, nullable=False)
    project_id = Column(Integer, nullable=False)

    database_id = Column(Integer, nullable=False)
    table_name = Column(String, nullable=False)
    query_str = Column(String, nullable=False)
    columns = Column(String)  # list of columns separated by delimiter

    updated_at = Column(
        DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now
    )
    created_at = Column(DateTime, default=datetime.datetime.now)


class Tasks(Base):
    __tablename__ = "tasks"
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

    updated_at = Column(
        DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now
    )
    created_at = Column(DateTime, default=datetime.datetime.now)


agent_skills_table = Table(
    "agent_skills",
    Base.metadata,
    Column("agent_id", ForeignKey("agents.id"), primary_key=True),
    Column("skill_id", ForeignKey("skills.id"), primary_key=True),
)


class Skills(Base):
    __tablename__ = "skills"
    id = Column(Integer, primary_key=True)
    agents: Mapped[List["Agents"]] = relationship(
        secondary=agent_skills_table, back_populates="skills"
    )
    name = Column(String, nullable=False)
    project_id = Column(Integer, nullable=False)
    type = Column(String, nullable=False)
    params = Column(JSON)

    created_at = Column(DateTime, default=datetime.datetime.now)
    updated_at = Column(
        DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now
    )
    deleted_at = Column(DateTime)

    def as_dict(self) -> Dict:
        return {
            "id": self.id,
            "name": self.name,
            "project_id": self.project_id,
            "agent_ids": [a.id for a in self.agents],
            "type": self.type,
            "params": self.params,
            "created_at": self.created_at,
        }


class Agents(Base):
    __tablename__ = "agents"
    id = Column(Integer, primary_key=True)
    skills: Mapped[List["Skills"]] = relationship(
        secondary=agent_skills_table, back_populates="agents"
    )
    company_id = Column(Integer, nullable=True)
    user_class = Column(Integer, nullable=True)

    name = Column(String, nullable=False)
    project_id = Column(Integer, nullable=False)

    model_name = Column(String, nullable=True)
    provider = Column(String, nullable=True)
    params = Column(JSON)

    updated_at = Column(
        DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now
    )
    created_at = Column(DateTime, default=datetime.datetime.now)
    deleted_at = Column(DateTime)

    def as_dict(self) -> Dict:
        return {
            "id": self.id,
            "name": self.name,
            "project_id": self.project_id,
            "model_name": self.model_name,
            "skills": [s.as_dict() for s in self.skills],
            "provider": self.provider,
            "params": self.params,
            "updated_at": self.updated_at,
            "created_at": self.created_at,
        }


class KnowledgeBase(Base):
    __tablename__ = "knowledge_base"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    project_id = Column(Integer, nullable=False)
    params = Column(JSON)

    vector_database_id = Column(
        ForeignKey("integration.id", name="fk_knowledge_base_vector_database_id"),
        doc="fk to the vector database integration",
    )
    vector_database = relationship(
        "Integration",
        foreign_keys=[vector_database_id],
        doc="vector database integration",
    )

    vector_database_table = Column(String, doc="table name in the vector database")

    embedding_model_id = Column(
        ForeignKey("predictor.id", name="fk_knowledge_base_embedding_model_id"),
        doc="fk to the embedding model",
    )

    embedding_model = relationship(
        "Predictor", foreign_keys=[embedding_model_id], doc="embedding model"
    )

    created_at = Column(DateTime, default=datetime.datetime.now)
    updated_at = Column(
        DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now
    )

    __table_args__ = (
        UniqueConstraint(
            "name", "project_id", name="unique_knowledge_base_name_project_id"
        ),
    )

    def as_dict(self) -> Dict:
        return {
            "id": self.id,
            "name": self.name,
            "project_id": self.project_id,
            "embedding_model": None if self.embedding_model is None else self.embedding_model.name,
            "vector_database": None if self.vector_database is None else self.vector_database.name,
            "vector_database_table": self.vector_database_table,
            "updated_at": self.updated_at,
            "created_at": self.created_at,
            "params": self.params
        }


class QueryContext(Base):
    __tablename__ = "query_context"
    id: int = Column(Integer, primary_key=True)
    company_id: int = Column(Integer, nullable=True)

    query: str = Column(String, nullable=False)
    context_name: str = Column(String, nullable=False)
    values: dict = Column(JSON)

    updated_at: datetime.datetime = Column(
        DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now
    )
    created_at: datetime.datetime = Column(DateTime, default=datetime.datetime.now)


class LLMLog(Base):
    __tablename__ = "llm_log"
    id: int = Column(Integer, primary_key=True)
    company_id: int = Column(Integer, nullable=True)
    api_key: str = Column(String, nullable=True)
    model_id: int = Column(Integer, nullable=False)
    input: str = Column(String, nullable=True)
    output: str = Column(String, nullable=True)
    start_time: datetime = Column(DateTime, nullable=False)
    end_time: datetime = Column(DateTime, nullable=True)
    prompt_tokens: int = Column(Integer, nullable=True)
    completion_tokens: int = Column(Integer, nullable=True)
    total_tokens: int = Column(Integer, nullable=True)
    success: bool = Column(Boolean, nullable=False, default=True)


class LLMData(Base):
    '''
    Stores the question/answer pairs of an LLM call so examples can be used
    for self improvement with DSPy
    '''
    __tablename__ = "llm_data"
    id: int = Column(Integer, primary_key=True)
    input: str = Column(String, nullable=False)
    output: str = Column(String, nullable=False)
    model_id: int = Column(Integer, nullable=False)
    created_at: datetime = Column(DateTime, default=datetime.datetime.now)
    updated_at: datetime = Column(DateTime, onupdate=datetime.datetime.now)

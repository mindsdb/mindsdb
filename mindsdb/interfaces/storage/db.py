import json
import datetime
from typing import Dict, List, Optional

import numpy as np
from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Index,
    Integer,
    LargeBinary,
    Numeric,
    String,
    UniqueConstraint,
    create_engine,
    text,
    types,
)
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import (
    Mapped,
    mapped_column,
    declarative_base,
    relationship,
    scoped_session,
    sessionmaker,
)
from sqlalchemy.sql.schema import ForeignKey

from mindsdb.utilities.json_encoder import CustomJSONEncoder
from mindsdb.utilities.config import config


class Base:
    __allow_unmapped__ = True


Base = declarative_base(cls=Base)

session, engine = None, None


def init(connection_str: str = None):
    global Base, session, engine
    if connection_str is None:
        connection_str = config["storage_db"]
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
    updated_at = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)
    created_at = Column(DateTime, default=datetime.datetime.now)
    deleted_at = Column(DateTime)
    name = Column(String)
    data = Column(Json)  # A JSON -- should be everything returned by `get_model_data`, I think
    to_predict = Column(Array)
    company_id = Column(Integer)
    mindsdb_version = Column(String)
    native_version = Column(String)
    integration_id = Column(ForeignKey("integration.id", name="fk_integration_id"))
    data_integration_ref = Column(Json)
    fetch_data_query = Column(String)
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
    project_id = Column(Integer, ForeignKey("project.id", name="fk_project_id"), nullable=False)
    training_phase_current = Column(Integer)
    training_phase_total = Column(Integer)
    training_phase_name = Column(String)
    training_metadata = Column(JSON, default={}, nullable=False)

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
    unique=True,
)


class Project(Base):
    __tablename__ = "project"

    id = Column(Integer, primary_key=True)
    created_at = Column(DateTime, default=datetime.datetime.now)
    updated_at = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)
    deleted_at = Column(DateTime)
    name = Column(String, nullable=False)
    company_id = Column(Integer, default=0)
    metadata_: dict = Column("metadata", JSON, nullable=True)
    __table_args__ = (UniqueConstraint("name", "company_id", name="unique_project_name_company_id"),)


class Integration(Base):
    __tablename__ = "integration"
    id = Column(Integer, primary_key=True)
    updated_at = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)
    created_at = Column(DateTime, default=datetime.datetime.now)
    name = Column(String, nullable=False)
    engine = Column(String, nullable=False)
    data = Column(Json)
    company_id = Column(Integer)

    meta_tables = relationship("MetaTables", back_populates="integration")

    __table_args__ = (UniqueConstraint("name", "company_id", name="unique_integration_name_company_id"),)


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
    metadata_: dict = Column("metadata", JSON, nullable=True)
    updated_at = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)
    __table_args__ = (UniqueConstraint("name", "company_id", name="unique_file_name_company_id"),)


class View(Base):
    __tablename__ = "view"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    company_id = Column(Integer)
    query = Column(String, nullable=False)
    project_id = Column(Integer, ForeignKey("project.id", name="fk_project_id"), nullable=False)
    __table_args__ = (UniqueConstraint("name", "company_id", name="unique_view_name_company_id"),)


class JsonStorage(Base):
    __tablename__ = "json_storage"
    id = Column(Integer, primary_key=True)
    resource_group = Column(String)
    resource_id = Column(Integer)
    name = Column(String)
    content = Column(JSON)
    encrypted_content = Column(LargeBinary, nullable=True)
    company_id = Column(Integer)

    def to_dict(self) -> Dict:
        return {
            "id": self.id,
            "resource_group": self.resource_group,
            "resource_id": self.resource_id,
            "name": self.name,
            "content": self.content,
            "encrypted_content": self.encrypted_content,
            "company_id": self.company_id,
        }


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
    updated_at = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)
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

    __table_args__ = (UniqueConstraint("job_id", "start_at", name="uniq_job_history_job_id_start"),)


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

    updated_at = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)
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

    updated_at = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)
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

    updated_at = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)
    created_at = Column(DateTime, default=datetime.datetime.now)


class AgentSkillsAssociation(Base):
    __tablename__ = "agent_skills"

    agent_id: Mapped[int] = mapped_column(ForeignKey("agents.id"), primary_key=True)
    skill_id: Mapped[int] = mapped_column(ForeignKey("skills.id"), primary_key=True)
    parameters: Mapped[dict] = mapped_column(JSON, default={}, nullable=True)

    agent = relationship("Agents", back_populates="skills_relationships")
    skill = relationship("Skills", back_populates="agents_relationships")


class Skills(Base):
    __tablename__ = "skills"
    id = Column(Integer, primary_key=True)
    agents_relationships: Mapped[List["Agents"]] = relationship(AgentSkillsAssociation, back_populates="skill")
    name = Column(String, nullable=False)
    project_id = Column(Integer, nullable=False)
    type = Column(String, nullable=False)
    params = Column(JSON)

    created_at = Column(DateTime, default=datetime.datetime.now)
    updated_at = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)
    deleted_at = Column(DateTime)

    def as_dict(self) -> Dict:
        return {
            "id": self.id,
            "name": self.name,
            "project_id": self.project_id,
            "agent_ids": [rel.agent.id for rel in self.agents_relationships],
            "type": self.type,
            "params": self.params,
            "created_at": self.created_at,
        }


class Agents(Base):
    __tablename__ = "agents"
    id = Column(Integer, primary_key=True)
    skills_relationships: Mapped[List["Skills"]] = relationship(AgentSkillsAssociation, back_populates="agent")
    company_id = Column(Integer, nullable=True)
    user_class = Column(Integer, nullable=True)

    name = Column(String, nullable=False)
    project_id = Column(Integer, nullable=False)

    model_name = Column(String, nullable=True)
    provider = Column(String, nullable=True)
    params = Column(JSON)

    updated_at = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)
    created_at = Column(DateTime, default=datetime.datetime.now)
    deleted_at = Column(DateTime)

    def as_dict(self) -> Dict:
        skills = []
        skills_extra_parameters = {}
        for rel in self.skills_relationships:
            skill = rel.skill
            # Skip auto-generated SQL skills
            if skill.params.get("description", "").startswith("Auto-generated SQL skill for agent"):
                continue
            skills.append(skill.as_dict())
            skills_extra_parameters[skill.name] = rel.parameters or {}

        params = self.params.copy()

        agent_dict = {
            "id": self.id,
            "name": self.name,
            "project_id": self.project_id,
            "updated_at": self.updated_at,
            "created_at": self.created_at,
        }

        if self.model_name:
            agent_dict["model_name"] = self.model_name

        if self.provider:
            agent_dict["provider"] = self.provider

        # Since skills were depreciated, they are only used with Minds
        # Minds expects the parameters to be provided as is without breaking them down
        if skills:
            agent_dict["skills"] = skills
            agent_dict["skills_extra_parameters"] = skills_extra_parameters
            agent_dict["params"] = params
        else:
            data = params.pop("data", {})
            model = params.pop("model", {})
            prompt_template = params.pop("prompt_template", None)
            if data:
                agent_dict["data"] = data
            if model:
                agent_dict["model"] = model
            if prompt_template:
                agent_dict["prompt_template"] = prompt_template
            if params:
                agent_dict["params"] = params

        return agent_dict


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

    embedding_model = relationship("Predictor", foreign_keys=[embedding_model_id], doc="embedding model")
    query_id = Column(Integer, nullable=True)

    created_at = Column(DateTime, default=datetime.datetime.now)
    updated_at = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)

    __table_args__ = (UniqueConstraint("name", "project_id", name="unique_knowledge_base_name_project_id"),)

    def as_dict(self, with_secrets: Optional[bool] = True) -> Dict:
        params = self.params.copy()
        embedding_model = params.pop("embedding_model", None)
        reranking_model = params.pop("reranking_model", None)

        if not with_secrets:
            if embedding_model and "api_key" in embedding_model:
                embedding_model["api_key"] = "******"

            if reranking_model and "api_key" in reranking_model:
                reranking_model["api_key"] = "******"

        return {
            "id": self.id,
            "name": self.name,
            "project_id": self.project_id,
            "vector_database": None if self.vector_database is None else self.vector_database.name,
            "vector_database_table": self.vector_database_table,
            "updated_at": self.updated_at,
            "created_at": self.created_at,
            "query_id": self.query_id,
            "embedding_model": embedding_model,
            "reranking_model": reranking_model,
            "metadata_columns": params.pop("metadata_columns", None),
            "content_columns": params.pop("content_columns", None),
            "id_column": params.pop("id_column", None),
            "params": params,
        }


class QueryContext(Base):
    __tablename__ = "query_context"
    id: int = Column(Integer, primary_key=True)
    company_id: int = Column(Integer, nullable=True)

    query: str = Column(String, nullable=False)
    context_name: str = Column(String, nullable=False)
    values: dict = Column(JSON)

    updated_at: datetime.datetime = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)
    created_at: datetime.datetime = Column(DateTime, default=datetime.datetime.now)


class Queries(Base):
    __tablename__ = "queries"
    id: int = Column(Integer, primary_key=True)
    company_id: int = Column(Integer, nullable=True)

    sql: str = Column(String, nullable=False)
    database: str = Column(String, nullable=True)

    started_at: datetime.datetime = Column(DateTime)
    finished_at: datetime.datetime = Column(DateTime)

    parameters = Column(JSON, default={})
    context = Column(JSON, default={})
    processed_rows = Column(Integer, default=0)
    error: str = Column(String, nullable=True)

    updated_at: datetime.datetime = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)
    created_at: datetime.datetime = Column(DateTime, default=datetime.datetime.now)


class LLMLog(Base):
    __tablename__ = "llm_log"
    id: int = Column(Integer, primary_key=True)
    company_id: int = Column(Integer, nullable=False)
    api_key: str = Column(String, nullable=True)
    model_id: int = Column(Integer, nullable=True)
    model_group: str = Column(String, nullable=True)
    input: str = Column(JSON, nullable=True)
    output: str = Column(JSON, nullable=True)
    start_time: datetime = Column(DateTime, nullable=False)
    end_time: datetime = Column(DateTime, nullable=True)
    cost: float = Column(Numeric(5, 2), nullable=True)
    prompt_tokens: int = Column(Integer, nullable=True)
    completion_tokens: int = Column(Integer, nullable=True)
    total_tokens: int = Column(Integer, nullable=True)
    success: bool = Column(Boolean, nullable=False, default=True)
    exception: str = Column(String, nullable=True)
    traceback: str = Column(String, nullable=True)
    stream: bool = Column(Boolean, default=False, comment="Is this completion done in 'streaming' mode")
    metadata_: dict = Column("metadata", JSON, nullable=True)


class LLMData(Base):
    """
    Stores the question/answer pairs of an LLM call so examples can be used
    for self improvement with DSPy
    """

    __tablename__ = "llm_data"
    id: int = Column(Integer, primary_key=True)
    input: str = Column(String, nullable=False)
    output: str = Column(String, nullable=False)
    model_id: int = Column(Integer, nullable=False)
    created_at: datetime = Column(DateTime, default=datetime.datetime.now)
    updated_at: datetime = Column(DateTime, onupdate=datetime.datetime.now)


# Data Catalog
class MetaTables(Base):
    __tablename__ = "meta_tables"
    id: int = Column(Integer, primary_key=True)

    integration_id: int = Column(Integer, ForeignKey("integration.id"))
    integration = relationship("Integration", back_populates="meta_tables")

    name: str = Column(String, nullable=False)
    schema: str = Column(String, nullable=True)
    description: str = Column(String, nullable=True)
    type: str = Column(String, nullable=True)
    row_count: int = Column(BigInteger, nullable=True)

    meta_columns: Mapped[List["MetaColumns"]] = relationship("MetaColumns", back_populates="meta_tables")
    meta_primary_keys: Mapped[List["MetaPrimaryKeys"]] = relationship("MetaPrimaryKeys", back_populates="meta_tables")
    meta_foreign_keys_parents: Mapped[List["MetaForeignKeys"]] = relationship(
        "MetaForeignKeys", foreign_keys="MetaForeignKeys.parent_table_id", back_populates="parent_table"
    )
    meta_foreign_keys_children: Mapped[List["MetaForeignKeys"]] = relationship(
        "MetaForeignKeys", foreign_keys="MetaForeignKeys.child_table_id", back_populates="child_table"
    )

    def as_string(self, indent: int = 0) -> str:
        pad = " " * indent

        table_info = f"`{self.integration.name}`.`{self.name}` ({self.type})"

        if self.description:
            table_info += f" : {self.description}"

        if self.schema:
            table_info += f"\n{pad}Schema: {self.schema}"

        if self.row_count and self.row_count > 0:
            table_info += f"\n{pad}Estimated Row Count: {self.row_count}"

        if self.meta_primary_keys:
            table_info += f"\n{pad}Primary Keys (in defined order): {', '.join([pk.as_string() for pk in self.meta_primary_keys])}"

        if self.meta_columns:
            table_info += f"\n\n{pad}Columns:"
            for index, column in enumerate(self.meta_columns, start=1):
                table_info += f"\n{index}. {column.as_string(indent + 4)}\n"

        if self.meta_foreign_keys_children:
            table_info += f"\n\n{pad}Key Relationships:"
            for fk in self.meta_foreign_keys_children:
                table_info += f"\n{pad}  {fk.as_string()}"

        return table_info


class MetaColumns(Base):
    __tablename__ = "meta_columns"
    id: int = Column(Integer, primary_key=True)

    table_id: int = Column(Integer, ForeignKey("meta_tables.id"))
    meta_tables = relationship("MetaTables", back_populates="meta_columns")

    name: str = Column(String, nullable=False)
    data_type: str = Column(String, nullable=False)
    description: str = Column(String, nullable=True)
    default_value: str = Column(String, nullable=True)
    is_nullable: bool = Column(Boolean, nullable=True)

    meta_column_statistics: Mapped[List["MetaColumnStatistics"]] = relationship(
        "MetaColumnStatistics", back_populates="meta_columns"
    )
    meta_primary_keys: Mapped[List["MetaPrimaryKeys"]] = relationship("MetaPrimaryKeys", back_populates="meta_columns")
    meta_foreign_keys_parents: Mapped[List["MetaForeignKeys"]] = relationship(
        "MetaForeignKeys", foreign_keys="MetaForeignKeys.parent_column_id", back_populates="parent_column"
    )
    meta_foreign_keys_children: Mapped[List["MetaForeignKeys"]] = relationship(
        "MetaForeignKeys", foreign_keys="MetaForeignKeys.child_column_id", back_populates="child_column"
    )

    def as_string(self, indent: int = 0) -> str:
        pad = " " * indent

        column_info = f"{self.name} ({self.data_type}):"
        if self.description:
            column_info += f"\n{pad}Description: {self.description}"

        if self.is_nullable:
            column_info += f"\n{pad}- Nullable: Yes"

        if self.default_value:
            column_info += f"\n{pad}- Default Value: {self.default_value}"

        stats = self.meta_column_statistics or []
        if stats and callable(getattr(stats[0], "as_string", None)):
            column_info += f"\n\n{pad}- Column Statistics:"
            column_info += f"\n{stats[0].as_string(indent + 4)}"
        return column_info


class MetaColumnStatistics(Base):
    __tablename__ = "meta_column_statistics"
    column_id: int = Column(Integer, ForeignKey("meta_columns.id"), primary_key=True)
    meta_columns = relationship("MetaColumns", back_populates="meta_column_statistics")

    most_common_values: str = Column(Array, nullable=True)
    most_common_frequencies: str = Column(Array, nullable=True)
    null_percentage: float = Column(Numeric(5, 2), nullable=True)
    distinct_values_count: int = Column(BigInteger, nullable=True)
    minimum_value: str = Column(String, nullable=True)
    maximum_value: str = Column(String, nullable=True)

    def as_string(self, indent: int = 0) -> str:
        pad = " " * indent
        inner_pad = " " * (indent + 4)

        column_statistics = ""
        most_common_values = self.most_common_values or []
        most_common_frequencies = self.most_common_frequencies or []

        if most_common_values and most_common_frequencies:
            column_statistics += f"{pad}- Top 10 Most Common Values and Frequencies:"
            for i in range(min(10, len(most_common_values))):
                freq = most_common_frequencies[i]
                try:
                    percent = float(freq) * 100
                    freq_str = f"{percent:.2f}%"
                except (ValueError, TypeError):
                    freq_str = str(freq)

                column_statistics += f"\n{inner_pad}- {most_common_values[i]}: {freq_str}"
            column_statistics += "\n"

        if self.null_percentage:
            column_statistics += f"{pad}- Null Percentage: {self.null_percentage}\n"

        if self.distinct_values_count:
            column_statistics += f"{pad}- No. of Distinct Values: {self.distinct_values_count}\n"

        if self.minimum_value:
            column_statistics += f"{pad}- Minimum Value: {self.minimum_value}\n"

        if self.maximum_value:
            column_statistics += f"{pad}- Maximum Value: {self.maximum_value}"

        return column_statistics


class MetaPrimaryKeys(Base):
    __tablename__ = "meta_primary_keys"
    table_id: int = Column(Integer, ForeignKey("meta_tables.id"), primary_key=True)
    meta_tables = relationship("MetaTables", back_populates="meta_primary_keys")

    column_id: int = Column(Integer, ForeignKey("meta_columns.id"), primary_key=True)
    meta_columns = relationship("MetaColumns", back_populates="meta_primary_keys")

    ordinal_position: int = Column(Integer, nullable=True)
    constraint_name: str = Column(String, nullable=True)

    def as_string(self) -> str:
        pk_list = sorted(
            self.meta_tables.meta_primary_keys,
            key=lambda pk: pk.ordinal_position if pk.ordinal_position is not None else 0,
        )

        return ", ".join(f"{pk.meta_columns.name} ({pk.meta_columns.data_type})" for pk in pk_list)


class MetaForeignKeys(Base):
    __tablename__ = "meta_foreign_keys"
    parent_table_id: int = Column(Integer, ForeignKey("meta_tables.id"), primary_key=True)
    parent_table = relationship(
        "MetaTables", back_populates="meta_foreign_keys_parents", foreign_keys=[parent_table_id]
    )

    parent_column_id: int = Column(Integer, ForeignKey("meta_columns.id"), primary_key=True)
    parent_column = relationship(
        "MetaColumns", back_populates="meta_foreign_keys_parents", foreign_keys=[parent_column_id]
    )

    child_table_id: int = Column(Integer, ForeignKey("meta_tables.id"), primary_key=True)
    child_table = relationship("MetaTables", back_populates="meta_foreign_keys_children", foreign_keys=[child_table_id])

    child_column_id: int = Column(Integer, ForeignKey("meta_columns.id"), primary_key=True)
    child_column = relationship(
        "MetaColumns", back_populates="meta_foreign_keys_children", foreign_keys=[child_column_id]
    )

    constraint_name: str = Column(String, nullable=True)

    def as_string(self) -> str:
        return f"{self.child_column.name} in {self.child_table.name} references {self.parent_column.name} in {self.parent_table.name}"

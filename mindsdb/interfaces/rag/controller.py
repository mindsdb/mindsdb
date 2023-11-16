from typing import List

from mindsdb_sql.parser.ast import (
    ASTNode,
    Delete,
    Identifier,
    Insert,
    Select,
    Update,
)

import mindsdb.interfaces.storage.db as db
from mindsdb.api.mysql.mysql_proxy.classes.sql_query import SQLQuery
from mindsdb.integrations.libs.vectordatabase_handler import TableField
from mindsdb.api.mysql.mysql_proxy.executor.data_types import ANSWER_TYPE, ExecuteAnswer


class RAGBaseController:
    """
    RAG controller handles all
    db related operations for RAGs
    """

    def __init__(self, session) -> None:
        self.executor = RAGBaseExecutor(session=session)
        self.session = session

    def is_rag(self, identifier: Identifier) -> bool:
        """
        Decide if the identifier is a RAG
        """
        return self.executor.is_rag(identifier)

    def execute_query(self, query: ASTNode) -> ExecuteAnswer:
        """
        Execute a parsed query and return the result
        """
        if isinstance(query, Select):
            return self.executor.select_from_rag(query)
        elif isinstance(query, Insert):
            raise NotImplementedError("Insert is not supported for RAGs")
        elif isinstance(query, Delete):
            raise NotImplementedError("Delete is not supported for RAGs")
        elif isinstance(query, Update):
            return self.executor.update_rag(query)
        else:
            raise NotImplementedError()

    def add(
            self,
            name: str,
            project_id: str,
            knowledge_base_id: str,
            llm_id: str,
            params: dict,
            if_not_exists: bool = False,
    ) -> int:
        """
        Add a new RAG to the database
        """
        # check if RAG already exists
        try:
            rag = self.get(name, project_id)
        except ValueError:
            # RAG does not exist
            rag = db.RAG(
                name=name,
                project_id=project_id,
                knowledge_base_id=knowledge_base_id,
                llm_id=llm_id,
                params=params,
            )
            db.session.add(rag)
            db.session.commit()
            return rag.id

        # RAG already exists
        if if_not_exists:
            return rag.id
        else:
            raise Exception(f"Knowledge base already exists: {name}")

    def delete(self, name: str, project_id: str, if_exists: bool = False) -> None:
        """
        Delete a RAG from the database
        """
        # check if RAG exists
        try:
            rag = self.get(name, project_id)
        except ValueError:
            # RAG does not exist
            if if_exists:
                return
            else:
                raise Exception(f"Knowledge base does not exist: {name}")

        # RAG exists
        db.session.delete(rag)
        db.session.commit()

    def get(self, name: str, project_id: str) -> db.RAG:
        """
        Get a RAG from the database
        by name + project_id
        """
        rag = (
            db.session.query(db.RAG)
            .filter_by(
                name=name,
                project_id=project_id,
            )
            .first()
        )
        if rag is None:
            raise ValueError(f"Knowledge base not found: {name}")
        return rag

    def get_by_id(self, id: str) -> db.RAG:
        """
        Get a RAG from the database
        by id
        """
        rag = (
            db.session.query(db.RAG)
            .filter_by(
                id=id,
            )
            .first()
        )
        if rag is None:
            raise ValueError(f"Knowledge base not found: {id}")
        return rag

    def list(self, project_id: str) -> List[db.RAG]:
        """
        List all RAG from the database
        belonging to a project
        """
        rags = (
            db.session.query(db.RAG)
            .filter_by(
                project_id=project_id,
            )
            .all()
        )
        return rags

    def update(self, name: str, project_id: str, **kwargs) -> db.RAG:
        """
        Update a RAG from the database
        """
        rag = self.get(name, project_id)
        for key, value in kwargs.items():
            setattr(rag, key, value)
        db.session.commit()
        return rag

    def update_by_id(self, id: str, **kwargs) -> db.RAG:
        """
        Update a RAG from the database by id
        """
        rag = self.get_by_id(id)
        for key, value in kwargs.items():
            setattr(rag, key, value)
        db.session.commit()
        return rag

    def list_rag_context_entry(self) -> List[dict]:
        """
        List all RAG context entries
        """
        # TODO: this is n+1 query, need to optimize
        rags = db.session.query(
            db.RAG,
        ).all()
        rag_context_entries = [self.get_rag_context_entry_by_id(rag.id) for rag in rags]
        return rag_context_entries

    def get_rag_context_entry_by_id(self, id: str) -> dict:
        """
        Get the rag context entry
        in the format of
        {
            "name": "my_rag",
            "type": "rag",
            "llm": "llm"

        }
        """
        rag = self.get_by_id(id)
        name = rag.name
        type_ = "RAG"

        # get llm
        llm_id = rag.llm_id

        # get the input columns
        llm = (
            db.session.query(db.Predictor)
            .filter_by(
                id=llm_id,
            )
            .first()
        )

        if llm is None:
            raise ValueError(f"Model not found: {llm_id}")

        llm_project = (
            db.session.query(db.Project)
            .filter_by(
                id=llm.project_id,
            )
            .first()
        )

        if llm_project is None:
            raise ValueError(f"Project not found: {llm.project_id}")

        llm_name = f"{llm_project.name}.{llm.name}"

        # get the output columns
        # describe the model
        args_df = self.session.model_controller.describe_model(
            session=self.session,
            project_name=llm_project.name,
            model_name=llm.name,
            attribute="args",
        )
        args_df.set_index("tables", inplace=True)

        # get the knowledge base id
        knowledge_base_id = rag.knowledge_base_id

        # get the knowledge_base object
        kb = (
            db.session.query(db.KnowledgeBase)
            .filter_by(
                id=knowledge_base_id,
            )
            .first()
        )

        if kb is None:
            raise ValueError(f"Knowledge Base not found: {knowledge_base_id}")

        return {
            "name": name,
            "type": type_,
            "llm": llm,
            "llm_name": llm_name,
            "knowledge_base": kb,
        }


class RAGBaseExecutor:
    """
    RAG base executor handles all queries for RAGs
    """

    KNOWLEDGE_BASE_FIELD = "knowledge_base"
    LLM_FIELD = "llm"

    def __init__(self, session) -> None:
        self.session = session

    def _get_rag_metadata(self, identifier: Identifier) -> dict:
        """
        Get the metadata of a RAG
        """
        name_parts = list(identifier.parts)
        name = name_parts[-1]
        if len(name_parts) > 1:
            namespace = name_parts[-2]
        else:
            namespace = self.session.database

        # query the db
        project_id = self.session.database_controller.get_project(namespace).id

        rag = self.session.rag_controller.get(
            name=name,
            project_id=project_id,
        )

        rag_metadata = self.session.rag_controller.get_rag_context_entry_by_id(
            id=rag.id,
        )

        return rag_metadata

    def is_rag(self, identifier: Identifier) -> bool:
        """
        Check if the identifier is a rag
        """
        try:
            self._get_rag_metadata(identifier)
            return True
        except (ValueError, AttributeError):
            return False

    def select_from_rag(self, query: Select) -> ExecuteAnswer:
        """
        Handle the select query
        We do the following translation logics:
        1. Select from the underlying storage table
        2. If a search query clause is provided in where, we
            substitute the search query clause with a nested select
            from the underlying model query
        """
        rag_metadata = self._get_rag_metadata(query.from_table)
        llm = rag_metadata[self.LLM_FIELD]
        kb = rag_metadata[self.KNOWLEDGE_BASE_FIELD]

        if not query.where and query.where.op != "=":
            raise ValueError("query on a RAG must include a single where clause")

        input_where_left = query.where.args[0]
        input_where_right = query.where.args[1]

        vector_db_content = TableField.CONTENT.value

        # build the search query for the underlying knowledge base
        query.targets = [Identifier(vector_db_content)]
        query.from_table = Identifier(kb.name)
        query.where.args[0] = Identifier("search_query")

        # search knowledge base with the search query
        kb_result = self.session.kb_controller.execute_query(query=query)

        content_data = ", ".join([data[0] for data in kb_result.data])

        # pass retrieved data from knowledgebase to llm
        llm_query = f"""
        select * from {llm.name} 
        where {vector_db_content}='{content_data}' 
        and {input_where_left}={input_where_right}
        """

        sql_query = SQLQuery(sql=llm_query, session=self.session, execute=True)
        data = sql_query.fetch()

        return ExecuteAnswer(
            answer_type=ANSWER_TYPE.TABLE,
            columns=sql_query.columns_list,
            data=data["result"],
        )

    def update_rag(self, query: Update) -> ExecuteAnswer:
        # TODO: to be implemented
        raise NotImplementedError()

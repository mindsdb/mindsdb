from typing import List

import pandas as pd
from mindsdb_sql.parser.ast import (
    ASTNode,
    Delete,
    Identifier,
    Insert,
    Select,
    Update,
)
from pandas import DataFrame

import mindsdb.interfaces.storage.db as db
from mindsdb.api.mysql.mysql_proxy.classes.sql_query import SQLQuery
from mindsdb.api.mysql.mysql_proxy.utilities import SqlApiException
from mindsdb.integrations.libs.vectordatabase_handler import TableField
from mindsdb.api.mysql.mysql_proxy.executor.data_types import ExecuteAnswer
from mindsdb.utilities.exception import EntityExistsError


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
            project_name: str,
            knowledge_base: Identifier,
            llm: Identifier,
            params: dict,
            if_not_exists: bool = False,
    ) -> int:
        """
        Add a new RAG to the database
        """

        # get project id
        project = self.session.database_controller.get_project(project_name)

        project_id = project.id

        # not difference between cases in sql
        name = name.lower()

        # check if RAG already exists
        rag = self.get(name, project_id)

        if rag is not None:
            if if_not_exists:
                return rag
            raise EntityExistsError("RAG already exists", name)

        if llm is None:
            raise SqlApiException("You must pass a llm model when creating RAG")
        else:
            if len(llm.parts) > 1:
                # llm project is set
                llm_project = self.session.database_controller.get_project(llm.parts[-2])
            else:
                llm_project = project

            llm_name = llm.parts[-1]
            llm = self.session.model_controller.get_model(name=llm_name, project_name=llm_project.name)

            llm_record = db.Predictor.query.get(llm['id'])
            llm_id = llm_record.id

        if knowledge_base is None:
            kb_name = f"default_kb_{name}"
            kb = self.session.kb_controller.add(
                name=kb_name,
                project_name=project_name,
                embedding_model=None,
                storage=None,
                params=params
            )

        else:
            if len(knowledge_base.parts) > 1:
                # kb project is set
                kb_project = self.session.database_controller.get_project(knowledge_base.parts[-2])
            else:
                kb_project = project

            kb_name = knowledge_base.parts[-1]
            kb = self.session.kb_controller.get(name=kb_name, project_id=kb_project.id)

        rag = db.RAG(
            name=name,
            project_id=project_id,
            knowledge_base_id=kb.id,
            llm_id=llm_id
        )

        db.session.add(rag)
        db.session.commit()
        return rag

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

    def select_from_rag(self, query: Select) -> DataFrame:
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
        kb_query = query.copy()

        kb_query.targets = [Identifier(vector_db_content)]
        kb_query.from_table = Identifier(kb.name)
        kb_query.where.args[0] = Identifier(vector_db_content)

        # search knowledge base with the search query
        kb_table = self.session.kb_controller.get_table(kb.name, kb.project_id)
        kb_result_df = kb_table.select_query(query=kb_query)

        content_data = ", ".join(kb_result_df[vector_db_content].to_list())

        # pass retrieved data from knowledgebase to llm
        llm_query = f"""
        select * from {llm.name} 
        where {vector_db_content}='{content_data}' 
        and {input_where_left}={input_where_right}
        """

        sql_query = SQLQuery(sql=llm_query, session=self.session, execute=True)
        data = sql_query.fetch()['result']
        columns = [column.name for column in sql_query.columns_list]

        return pd.DataFrame(data=data, columns=columns)

    def update_rag(self, query: Update) -> ExecuteAnswer:
        # TODO: to be implemented
        raise NotImplementedError()

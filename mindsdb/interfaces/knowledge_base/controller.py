from typing import List

import pandas as pd

import mindsdb_sql.planner.utils as utils
from mindsdb_sql.parser.ast import (
    BinaryOperation,
    Constant,
    Identifier,
    Select,
    Update,
)

import mindsdb.interfaces.storage.db as db
from mindsdb.integrations.libs.vectordatabase_handler import TableField


class KnowledgeBaseTable:
    def __init__(self, kb, session):
        self._kb = kb
        self._vector_db = None
        self.session = session

    def select_query(self, query: Select):
        # replace content with embeddings
        utils.query_traversal(query.where, self._replace_query_content)

        # set table name
        query.from_table = Identifier(parts=[self._kb.vector_database_table])

        # send to vectordb
        db_handler = self._get_vector_db()
        resp = db_handler.query(query)
        return resp.data_frame

    def update_query(self, query: Update):
        # add embeddings to content in updated collumns
        emb_col = TableField.EMBEDDINGS.value
        cont_col = TableField.CONTENT.value
        if cont_col in query.update_columns:
            content = query.update_columns[cont_col]
            query.update_columns[emb_col] = self._to_embeddings(content)

        # TODO search content in where clause?

        # set table name
        query.table = Identifier(parts=[self._kb.vector_database_table])

        # send to vectordb
        db_handler = self._get_vector_db()
        return db_handler.query(query)

    def _replace_query_content(self, node, **kwargs):
        if isinstance(node, BinaryOperation):
            if isinstance(node.args[0], Identifier) and isinstance(node.args[1], Constant):
                col_name = node.args[0].parts[-1]
                if col_name.lower() == TableField.CONTENT.value:
                    # replace
                    node.args[0].parts = [TableField.EMBEDDINGS.value]
                    node.args[1].value = [self._to_embeddings(node.args[1].value)]

    def insert(self, df):
        # add embeddings
        df_emb = self._add_embeddings(df)
        df = pd.concat([df, df_emb], axis=1)

        # send to vector db
        db_handler = self._get_vector_db()
        db_handler.do_upsert(self._kb.vector_database_table, df)

    def _get_vector_db(self):
        if self._vector_db is None:
            database_name = db.Integration.query.get(self._kb.vector_database_id).name
            self._vector_db = self.session.integration_controller.get_handler(database_name)
        return self._vector_db

    def _call_embeddings_model(self, data):
        model_id = self._kb.embedding_model_id
        # get the input columns
        model_rec = db.session.query(db.Predictor).filter_by(id=model_id).first()

        assert model_rec is not None, f"Model not found: {model_id}"
        model_project = db.session.query(db.Project).filter_by(id=model_rec.project_id).first()

        project_datanode = self.session.datahub.get(model_project.name)

        # TODO adjust input
        input_col = model_rec.learn_args.get('using', {}).get('question_column')
        if input_col is not None and input_col != TableField.CONTENT.value:
            df = pd.DataFrame(data)
            df = df.rename(columns={TableField.CONTENT.value: input_col})
            data = df.to_dict('records')

        df_out = project_datanode.predict(
            model_name=model_rec.name,
            data=data,
        )

        target = model_rec.to_predict[0]
        if target != TableField.EMBEDDINGS.value:
            # adapt output for vectordb
            df_out = df_out.rename(columns={target: TableField.EMBEDDINGS.value})

        return df_out

    def _add_embeddings(self, df):
        # add embeddings to dataframe

        data = df.to_dict('records')
        return self._call_embeddings_model(data)

    def _to_embeddings(self, content):
        data = [
            {TableField.CONTENT.value: content}
        ]
        res = self._call_embeddings_model(data)
        return res[TableField.EMBEDDINGS.value][0]

    def delete(self):
        # TODO
        ...


class KnowledgeBaseController:
    """
    Knowledge bae controller handles all
    db related operations for knowledge bases
    """

    def __init__(self, session) -> None:
        self.session = session

    # def is_knowledge_base(self, identifier: Identifier) -> bool:
    #     """
    #     Decide if the identifier is a knowledge base
    #     """
    #     return self.executor.is_knowledge_base(identifier)

    # def execute_query(self, query: ASTNode) -> ExecuteAnswer:
    #     """
    #     Execute a parsed query and return the result
    #     """
    #     if isinstance(query, Select):
    #         return self.executor.select_from_kb(query)
    #     elif isinstance(query, Insert):
    #         return self.executor.insert_into_kb(query)
    #     elif isinstance(query, Delete):
    #         return self.executor.delete_from_kb(query)
    #     elif isinstance(query, Update):
    #         return self.executor.update_kb(query)
    #     else:
    #         raise NotImplementedError()

    def add(
        self,
        name: str,
        project_name: str,
        embedding_model: Identifier,
        storage: Identifier,
        params: dict,
        if_not_exists: bool = False,
    ):
        """
        Add a new knowledge base to the database
        """
        # check if knowledge base already exists

        # get project id

        project = self.session.database_controller.get_project(project_name)

        project_id = project.id

        # not difference between cases in sql
        name = name.lower()

        kb = self.get(name, project_id)
        if kb is not None:
            if if_not_exists:
                return kb
            else:
                raise Exception(f"Knowledge base already exists: {name}")

        # model
        model_name = embedding_model.parts[-1]

        if len(embedding_model.parts) > 1:
            # model project is set
            model_project = self.session.database_controller.get_project(embedding_model.parts[-2])
        else:
            model_project = project

        model = self.session.model_controller.get_model(
            model_name,
            project_name=model_project.name
        )
        model_record = db.Predictor.query.get(model['id'])
        embedding_model_id = model_record.id

        # search for the vector database table
        if storage is None:
            # create chroma db with same name
            vector_table_name = "default_collection"
            vector_db_name = self._create_persistent_chroma(
                name
            )
        elif len(storage.parts) != 2:
            raise Exception('Storage param has to be vector db with table')
        else:
            vector_db_name, vector_table_name = storage.parts

        vector_database_id = self.session.integration_controller.get(vector_db_name)['id']

        # create table in vectordb
        self.session.datahub.get(vector_db_name).integration_handler.create_table(
            vector_table_name
        )

        kb = db.KnowledgeBase(
            name=name,
            project_id=project_id,
            vector_database_id=vector_database_id,
            vector_database_table=vector_table_name,
            embedding_model_id=embedding_model_id,
            params=params,
        )
        db.session.add(kb)
        db.session.commit()
        return kb

    def _create_persistent_chroma(self, kb_name, engine="chromadb"):
        """Create default vector database for knowledge base, if not specified"""

        vector_store_name = f"{kb_name}_{engine}"

        vector_store_folder_name = f"{vector_store_name}"
        connection_args = {"persist_directory": vector_store_folder_name}

        # check if exists
        if self.session.integration_controller.get(vector_store_name):
            return vector_store_name

        self.session.integration_controller.add(vector_store_name, engine, connection_args)
        return vector_store_name

    def delete(self, name: str, project_name: str, if_exists: bool = False) -> None:
        """
        Delete a knowledge base from the database
        """
        try:
            project = self.session.database_controller.get_project(project_name)
        except ValueError:
            raise Exception(f"Project not found: {project_name}")
        project_id = project.id

        # check if knowledge base exists
        kb = self.get(name, project_id)
        if kb is None:
            # knowledge base does not exist
            if if_exists:
                return
            else:
                raise Exception(f"Knowledge base does not exist: {name}")

        # kb exists
        db.session.delete(kb)
        db.session.commit()

    def get(self, name: str, project_id: str):
        """
        Get a knowledge base from the database
        by name + project_id
        """
        kb = (
            db.session.query(db.KnowledgeBase)
            .filter_by(
                name=name,
                project_id=project_id,
            )
            .first()
        )
        return kb

    def get_table(self, name: str, project_id: str):
        kb = self.get(name, project_id)
        if kb is not None:
            return KnowledgeBaseTable(kb, self.session)

    # def get_by_id(self, id: str) -> db.KnowledgeBase:
    #     """
    #     Get a knowledge base from the database
    #     by id
    #     """
    #     kb = (
    #         db.session.query(db.KnowledgeBase)
    #         .filter_by(
    #             id=id,
    #         )
    #         .first()
    #     )
    #     if kb is None:
    #         raise ValueError(f"Knowledge base not found: {id}")
    #     return kb

    def list(self, project_id: str) -> List[db.KnowledgeBase]:
        """
        List all knowledge bases from the database
        belonging to a project
        """
        kbs = (
            db.session.query(db.KnowledgeBase)
            .filter_by(
                project_id=project_id,
            )
            .all()
        )
        return kbs

    def update(self, name: str, project_id: str, **kwargs) -> db.KnowledgeBase:
        """
        Update a knowledge base from the database
        """
        raise NotImplementedError()

    # def update_by_id(self, id: str, **kwargs) -> db.KnowledgeBase:
    #     """
    #     Update a knowledge base from the database
    #     """
    #     raise NotImplementedError()

    # def list_kb_context_entry(self) -> List[dict]:
    #     """
    #     List all knowledge base context entries
    #     """
    #     # TODO: this is n+1 query, need to optimize
    #     kbs = db.session.query(
    #         db.KnowledgeBase,
    #         # TODO Won't work on cloud need to filter by company
    #     ).all()
    #     kb_context_entries = [self.get_kb_context_entry_by_id(kb.id) for kb in kbs]
    #     return kb_context_entries

    # def get_kb_context_entry_by_id(self, id: str) -> dict:
    #     """
    #     Get the knowledge base context entry
    #     in the format of
    #     {
    #         "name": "my_kb",
    #         "type": "knowledge_base",
    #         "model": "mindsdb.my_model",
    #         "storage": "my_chromadb.my_table",
    #         "search_vector_field": "search_vector",
    #         "embeddings_field": "embeddings",
    #         "content_field": "content",
    #     }
    #     """
    #     # TODO where this function is going to be used?
    #
    #     kb = self.get_by_id(id)
    #     name = kb.name
    #     type_ = "knowledge_base"
    #     search_vector_field = "search_vector"
    #
    #     # get model
    #     model_id = kb.embedding_model_id
    #     # get the input columns
    #     model = (
    #         db.session.query(db.Predictor)
    #         .filter_by(
    #             id=model_id,
    #         )
    #         .first()
    #     )
    #
    #     assert model is not None, f"Model not found: {model_id}"
    #     model_project = (
    #         db.session.query(db.Project)
    #         .filter_by(
    #             id=model.project_id,
    #         )
    #         .first()
    #     )
    #
    #     assert model_project is not None, f"Project not found: {model.project_id}"
    #     model_name = f"{model_project.name}.{model.name}"
    #
    #     # get the output columns
    #     # describe the model
    #     args_df = self.session.model_controller.describe_model(
    #         session=self.session,
    #         project_name=model_project.name,
    #         model_name=model.name,
    #         attribute="args",
    #     )
    #     args_df.set_index("key", inplace=True)
    #     embeddings_field = args_df.loc["target", "value"]
    #     # TODO: assuming there is only one input column
    #     # need to support multiple input columns
    #     content_field = args_df.loc["input_columns", "value"][0]
    #
    #     # get the vector database name + table
    #     database_id = kb.vector_database_id
    #     table_name = kb.vector_database_table
    #     # get the database object
    #     database = (
    #         db.session.query(db.Integration)
    #         .filter_by(
    #             id=database_id,
    #         )
    #         .first()
    #     )
    #
    #     assert database is not None, f"Database not found: {database_id}"
    #
    #     storage = f"{database.name}.{table_name}"
    #     return {
    #         "name": name,
    #         "type": type_,
    #         "model": model_name,
    #         "storage": storage,
    #         "search_vector_field": search_vector_field,
    #         "embeddings_field": embeddings_field,
    #         "content_field": content_field,
    #     }

#
# class KnowledgeBaseExecutor:
#     """
#     Knowledge base executor handles all
#     sql related operations for knowledge bases
#     """
#
#     MODEL_FIELD = "model"
#     STORAGE_FIELD = "storage"
#     SEARCH_QUERY = "search_query"
#
#     def __init__(self, session) -> None:
#         self.session = session
#
#     def _get_knowledge_base_metadata(self, identifier: Identifier) -> dict:
#         """
#         Get the metadata of a knowledge base
#         """
#         name_parts = list(identifier.parts)
#         name = name_parts[-1]
#         if len(name_parts) > 1:
#             namespace = name_parts[-2]
#         else:
#             namespace = self.session.database
#
#         # query the db
#         project_id = self.session.database_controller.get_project(namespace).id
#
#         kb = self.session.kb_controller.get(
#             name=name,
#             project_id=project_id,
#         )
#
#         kb_metadata = self.session.kb_controller.get_kb_context_entry_by_id(
#             id=kb.id,
#         )
#
#         return kb_metadata
#
#     def is_knowledge_base(self, identifier: Identifier) -> bool:
#         """
#         Check if the identifier is a knowledge base
#         """
#         try:
#             self._get_knowledge_base_metadata(identifier)
#             return True
#         except (ValueError, AttributeError):
#             return False
#
#     def select_from_kb(self, query: Select) -> ExecuteAnswer:
#         """
#         Handle the select query
#         We do the following translation logics:
#         1. Select from the underlying storage table
#         2. If a search query clause is provided in where, we
#             substitute the search query clause with a nested select
#             from the underlying model query
#         """
#         knowledge_base_metadata = self._get_knowledge_base_metadata(query.from_table)
#         vector_database_table = knowledge_base_metadata[self.STORAGE_FIELD]
#         model_name = knowledge_base_metadata[self.MODEL_FIELD]
#
#         CONTENT_FIELD = (
#             knowledge_base_metadata.get("content_field") or TableField.CONTENT.value
#         )
#         EMBEDDINGS_FIELD = (
#             knowledge_base_metadata.get("embeddings_field")
#             or TableField.EMBEDDINGS.value
#         )
#         SEARCH_VECTOR_FIELD = (
#             knowledge_base_metadata.get("search_vector_field")
#             or TableField.SEARCH_VECTOR.value
#         )
#
#         is_search_query_present = False
#
#         def find_search_query(node, **kwargs):
#             nonlocal is_search_query_present
#             if isinstance(node, Identifier) and node.parts[-1] == self.SEARCH_QUERY:
#                 is_search_query_present = True
#
#         # decide predictor is needed in the query
#         # by detecting if a where clause involving field SEARCH_QUERY is present
#         # if yes, then we need to add additional step to the plan
#         # to apply the predictor to the search query
#         utils.query_traversal(query.where, callback=find_search_query)
#
#         if not is_search_query_present:
#             # dispatch to the underlying storage table
#             query.from_table = Identifier(vector_database_table)
#         else:
#             # rewrite the where clause
#             # search_query = 'some text'
#             # ->
#             # search_vector = (select embeddings from model_name where content = 'some text')
#             def rewrite_search_query_clause(node, **kwargs):
#                 if isinstance(node, BinaryOperation):
#                     if node.args[0] == Identifier(self.SEARCH_QUERY):
#                         node.args[0] = Identifier(SEARCH_VECTOR_FIELD)
#                         node.args[1] = Select(
#                             targets=[Identifier(EMBEDDINGS_FIELD)],
#                             from_table=Identifier(model_name),
#                             where=BinaryOperation(
#                                 op="=", args=[Identifier(CONTENT_FIELD), node.args[1]]
#                             ),
#                         )
#
#             utils.query_traversal(query.where, callback=rewrite_search_query_clause)
#
#             # dispatch to the underlying storage table
#             query.from_table = Identifier(vector_database_table)
#         sql_query = SQLQuery(sql=query, session=self.session, execute=True)
#         data = sql_query.fetch()
#
#         return ExecuteAnswer(
#             answer_type=ANSWER_TYPE.TABLE,
#             columns=sql_query.columns_list,
#             data=data["result"],
#         )
#
#     def insert_into_kb(self, query: Insert):
#         """
#         Handle the insert query
#         We do the following translation logics:
#         1. Insert into the underlying storage table
#         2. If a select query is present, we join the select query
#             with a model to get the embeddings column
#         3. If values are present, we wrap the values in ast.Data
#             join them with the model to get the embeddings column
#         """
#         metadata = self._get_knowledge_base_metadata(query.table)
#         EMBEDDINGS_FIELD = (
#             metadata.get("embeddings_field") or TableField.EMBEDDINGS.value
#         )
#
#         vector_database_table = metadata[self.STORAGE_FIELD]
#         model_name = metadata[self.MODEL_FIELD]
#
#         query.table = Identifier(vector_database_table)
#
#         if query.from_select is not None:
#             # detect if embeddings field is present in the columns list
#             # if so, we do not need to apply the predictor
#             # if not, we need to join the select with the model table
#             is_embeddings_field_present = False
#
#             def find_embeddings_field(node, **kwargs):
#                 nonlocal is_embeddings_field_present
#                 if isinstance(node, Identifier) and node.parts[-1] == EMBEDDINGS_FIELD:
#                     is_embeddings_field_present = True
#
#             utils.query_traversal(query.columns, callback=find_embeddings_field)
#
#             if is_embeddings_field_present:
#                 return self.plan_insert(query)
#
#             # rewrite the select statement
#             # to join with the model table
#
#             select: Select = query.from_select
#             select.targets.append(Identifier(EMBEDDINGS_FIELD))
#             select.from_table = Select(
#                 targets=copy.deepcopy(select.targets),
#                 from_table=Join(
#                     left=select.from_table,
#                     right=Identifier(model_name),
#                     join_type="JOIN",
#                 ),
#             )
#
#             # append the embeddings field to the columns list
#             if query.columns:
#                 query.columns.append(Identifier(EMBEDDINGS_FIELD))
#
#             return self.plan_insert(query)
#         else:
#             if not query.columns:
#                 raise Exception(
#                     "Insert into knowledge base requires a select query or a list of columns"
#                 )
#
#             keys = [column.name for column in query.columns]
#             is_embeddings_field_present = EMBEDDINGS_FIELD in keys
#
#             query.table = Identifier(vector_database_table)
#             # directly dispatch to the underlying storage table
#             if is_embeddings_field_present:
#                 return self.plan_insert(query)
#
#             # if the embeddings field is not present in the columns list
#             # we need to wrap values in ast.Data
#             # join it with a model table
#             # modify the query using from_table
#             # and dispatch to the underlying storage table
#
#             records = []
#             _unwrap_constant_or_self = (
#                 lambda node: node.value if isinstance(node, Constant) else node
#             )
#             for row in query.values:
#                 records.append(dict(zip(keys, map(_unwrap_constant_or_self, row))))
#
#             data = Data(records, alias=Identifier("data"))
#             predictor_select = Select(
#                 targets=[Identifier(col.name) for col in query.columns]
#                 + [Identifier(EMBEDDINGS_FIELD)],
#                 from_table=Join(
#                     left=data, right=Identifier(model_name), join_type="JOIN"
#                 ),
#             )
#
#             query.columns += [TableColumn(name=EMBEDDINGS_FIELD)]
#             query.from_select = predictor_select
#             query.values = None
#
#             _ = SQLQuery(sql=query, session=self.session, execute=True)
#             return ExecuteAnswer(
#                 answer_type=ANSWER_TYPE.OK,
#             )
#
#     def plan_insert(self, query: Insert):
#         # TODO: to be implemented
#         raise NotImplementedError()
#
#     def delete_from_kb(self, query: Delete):
#         metadata = self._get_knowledge_base_metadata(query.table)
#
#         vector_database_table = metadata[self.STORAGE_FIELD]
#         query.table = Identifier(vector_database_table)
#
#         _ = SQLQuery(sql=query, session=self.session, execute=True)
#         return ExecuteAnswer(
#             answer_type=ANSWER_TYPE.OK,
#         )
#
#     def update_kb(self, query: Update) -> ExecuteAnswer:
#         # TODO: to be implemented
#         raise NotImplementedError()

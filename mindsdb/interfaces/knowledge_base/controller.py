import copy
from typing import List

import pandas as pd

import mindsdb_sql.planner.utils as utils
from mindsdb_sql.parser.ast import (
    BinaryOperation,
    Constant,
    Identifier,
    Select,
    Update,
    Delete,
    Star
)
from mindsdb_sql.parser.dialects.mindsdb import CreatePredictor

import mindsdb.interfaces.storage.db as db
from mindsdb.integrations.libs.vectordatabase_handler import TableField
from mindsdb.interfaces.database.projects import ProjectController
from mindsdb.utilities.exception import EntityExistsError, EntityNotExistsError


class KnowledgeBaseTable:
    """
    Knowledge base table interface
    Handlers requests to KB table and modifies data in linked vector db table
    """

    def __init__(self, kb: db.KnowledgeBase, session):
        self._kb = kb
        self._vector_db = None
        self.session = session

    def select_query(self, query: Select) -> pd.DataFrame:
        """
        Handles select from KB table.
        Replaces content values with embeddings in where clause. Sends query to vector db
        :param query: query to KB table
        :return: dataframe with the result table
        """

        # replace content with embeddings

        utils.query_traversal(query.where, self._replace_query_content)

        # set table name
        query.from_table = Identifier(parts=[self._kb.vector_database_table])

        # remove embeddings from result
        targets = []
        for target in query.targets:
            if isinstance(target, Star):
                targets.extend([
                    Identifier(TableField.ID.value),
                    Identifier(TableField.CONTENT.value),
                    Identifier(TableField.METADATA.value),
                ])
            elif isinstance(target, Identifier) and target.parts[-1].lower() != TableField.EMBEDDINGS.value:
                targets.append(target)
        query.targets = targets

        # send to vectordb
        db_handler = self._get_vector_db()
        resp = db_handler.query(query)
        return resp.data_frame

    def update_query(self, query: Update):
        """
        Handles update query to KB table.
        Replaces content values with embeddings in SET clause. Sends query to vector db
        :param query: query to KB table
        """

        # add embeddings to content in updated collumns
        query = copy.deepcopy(query)

        emb_col = TableField.EMBEDDINGS.value
        cont_col = TableField.CONTENT.value
        if cont_col in query.update_columns:
            content = query.update_columns[cont_col]
            query.update_columns[emb_col] = Constant(self._content_to_embeddings(content))

        # TODO search content in where clause?

        # set table name
        query.table = Identifier(parts=[self._kb.vector_database_table])

        # send to vectordb
        db_handler = self._get_vector_db()
        db_handler.query(query)

    def delete_query(self, query: Delete):
        """
        Handles delete query to KB table.
        Replaces content values with embeddings in WHERE clause. Sends query to vector db
        :param query: query to KB table
        """
        utils.query_traversal(query.where, self._replace_query_content)

        # set table name
        query.table = Identifier(parts=[self._kb.vector_database_table])

        # send to vectordb
        db_handler = self._get_vector_db()
        db_handler.query(query)

    def clear(self):
        """
        Clear data in KB table
        Sends delete to vector db table
        """
        db_handler = self._get_vector_db()
        db_handler.delete(self._kb.vector_database_table)

    def insert(self, df: pd.DataFrame):
        """
        Insert dataframe to KB table
        Adds embedding column to dataframe and calls .upsert method of vector db
        :param df: input dataframe

        """
        if df.empty:
            return

        df = self._adapt_column_names(df)

        # add embeddings
        df_emb = self._df_to_embeddings(df)
        df = pd.concat([df, df_emb], axis=1)

        # send to vector db
        db_handler = self._get_vector_db()
        db_handler.do_upsert(self._kb.vector_database_table, df)

    def _adapt_column_names(self, df: pd.DataFrame) -> pd.DataFrame:

        '''
            convert input columns for vector db input
            - id, content and metadata
        '''

        params = self._kb.params

        columns = list(df.columns)

        # -- prepare id --

        # if id_column is defined:
        #     use it as id
        # elif 'id' column exists:
        #     use it
        # else:
        #     use hash(content) -- it happens inside of vector handler

        id_column = params.get('id_column')
        if id_column is not None and id_column not in columns:
            # wrong name
            id_column = None

        if id_column is None and TableField.ID.value in columns:
            # default value
            id_column = TableField.ID.value

        if id_column is not None:
            # remove from lookup list
            columns.remove(id_column)

        # -- prepare content and metadata --

        # if content_columns is defined:
        #     if len(content_columns) > 1:
        #          make text from row (col: value\n col: value)
        #     if metadata_columns is defined:
        #          use them as metadata
        #     else:
        #          use all unused columns is metadata
        #     elif metadata_columns is defined:
        #          metadata_columns go to metadata
        #          use all unused columns  as content (make text if columns>1)
        # else:
        #     no metadata
        #     all unused columns go to content (make text if columns>1)

        content_columns = params.get('content_columns')
        metadata_columns = params.get('metadata_columns')

        if content_columns is not None:
            content_columns = list(set(content_columns).intersection(columns))
            if len(content_columns) == 0:
                raise ValueError(f'Content columns {params.get("content_columns")} not found in dataset: {columns}')

            if metadata_columns is not None:
                metadata_columns = list(set(metadata_columns).intersection(columns))
            else:
                # all the rest columns
                metadata_columns = list(set(columns).difference(content_columns))

        elif metadata_columns is not None:
            metadata_columns = list(set(metadata_columns).intersection(columns))
            # use all unused columns is content
            content_columns = list(set(columns).difference(metadata_columns))
        else:
            # all columns go to content
            content_columns = columns

        if not content_columns:
            raise ValueError("Can't find content columns")

        def row_to_document(row: pd.Series) -> str:
            """
            Convert a row in the input dataframe into a document

            Default implementation is to concatenate all the columns
            in the form of
            field1: value1\nfield2: value2\n...
            """
            fields = row.index.tolist()
            values = row.values.tolist()
            document = "\n".join(
                [f"{field}: {value}" for field, value in zip(fields, values)]
            )
            return document

        # create dataframe
        if len(content_columns) == 1:
            c_content = df[content_columns[0]]
        else:
            c_content = df[content_columns].apply(row_to_document, axis=1)
        c_content.name = TableField.CONTENT.value
        df_out = pd.DataFrame(c_content)

        if id_column is not None:
            df_out[TableField.ID.value] = df[id_column]

        if metadata_columns and len(metadata_columns) > 0:
            df_out[TableField.METADATA.value] = df[metadata_columns].apply(lambda row: str(dict(row)), axis=1)

        return df_out

    def _replace_query_content(self, node, **kwargs):
        if isinstance(node, BinaryOperation):
            if isinstance(node.args[0], Identifier) and isinstance(node.args[1], Constant):
                col_name = node.args[0].parts[-1]
                if col_name.lower() == TableField.CONTENT.value:
                    # replace
                    node.args[0].parts = [TableField.EMBEDDINGS.value]
                    node.args[1].value = [self._content_to_embeddings(node.args[1].value)]

    def _get_vector_db(self):
        """
        helper to get vector db handler
        """
        if self._vector_db is None:
            database = db.Integration.query.get(self._kb.vector_database_id)
            if database is None:
                raise ValueError('Vector database not found. Is it deleted?')
            database_name = database.name
            self._vector_db = self.session.integration_controller.get_data_handler(database_name)
        return self._vector_db

    def _df_to_embeddings(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Returns embeddings for input dataframe.
        Uses model embedding model to convert content to embeddings.
        Automatically detects input and output of model using model description
        :param df:
        :return: dataframe with embeddings
        """

        if df.empty:
            return pd.DataFrame([], columns=[TableField.EMBEDDINGS.value])

        model_id = self._kb.embedding_model_id
        # get the input columns
        model_rec = db.session.query(db.Predictor).filter_by(id=model_id).first()

        assert model_rec is not None, f"Model not found: {model_id}"
        model_project = db.session.query(db.Project).filter_by(id=model_rec.project_id).first()

        project_datanode = self.session.datahub.get(model_project.name)

        # keep only content
        df = df[[TableField.CONTENT.value]]

        input_col = model_rec.learn_args.get('using', {}).get('question_column')

        if input_col is not None and input_col != TableField.CONTENT.value:
            df = df.rename(columns={TableField.CONTENT.value: input_col})

        df_out = project_datanode.predict(
            model_name=model_rec.name,
            df=df,
        )

        target = model_rec.to_predict[0]
        if target != TableField.EMBEDDINGS.value:
            # adapt output for vectordb
            df_out = df_out.rename(columns={target: TableField.EMBEDDINGS.value})
        df_out = df_out[[TableField.EMBEDDINGS.value]]

        return df_out

    def _content_to_embeddings(self, content: str) -> List[float]:
        """
        Converts string to embeddings
        :param content: input string
        :return: embeddings
        """
        df = pd.DataFrame([[content]], columns=[TableField.CONTENT.value])
        res = self._df_to_embeddings(df)
        return res[TableField.EMBEDDINGS.value][0]


class KnowledgeBaseController:
    """
    Knowledge base controller handles all
    manages knowledge bases
    """

    def __init__(self, session) -> None:
        self.session = session

    def add(
        self,
        name: str,
        project_name: str,
        embedding_model: Identifier,
        storage: Identifier,
        params: dict,
        if_not_exists: bool = False,
    ) -> db.KnowledgeBase:
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
            raise EntityExistsError("Knowledge base already exists", name)

        if embedding_model is None:
            # create default embedding model
            model_name = self._create_default_embedding_model(project.name, name, params=params)

            # memorize to remove it later
            params['embedding_model'] = model_name

        else:
            # get embedding model from input
            model_name = embedding_model.parts[-1]

        if embedding_model is not None and len(embedding_model.parts) > 1:
            # model project is set
            model_project = self.session.database_controller.get_project(embedding_model.parts[-2])
        else:
            model_project = project

        model = self.session.model_controller.get_model(
            name=model_name,
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

            # memorize to remove it later
            params['vector_storage'] = vector_db_name
        elif len(storage.parts) != 2:
            raise ValueError('Storage param has to be vector db with table')
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

    def _create_default_embedding_model(self, project_name, kb_name, engine="langchain_embedding", params: dict = None):
        """create a default embedding model for knowledge base, if not specified"""
        model_name = f"{kb_name}_default_model"
        using_args = {}
        if engine == 'langchain_embedding':
            # Use default embeddings.
            using_args['class'] = 'openai'
        # Include API key if provided.
        using_args.update({k: v for k, v in params.items() if 'api_key' in k})
        statement = CreatePredictor(
            name=Identifier(parts=[project_name, model_name]),
            using=using_args,
            targets=[
                Identifier(parts=[TableField.EMBEDDINGS.value])
            ]
        )
        ml_handler = self.session.integration_controller.get_ml_handler(engine)

        _ = self.session.model_controller.create_model(
            statement,
            ml_handler
        )

        return model_name

    def delete(self, name: str, project_name: int, if_exists: bool = False) -> None:
        """
        Delete a knowledge base from the database
        """
        try:
            project = self.session.database_controller.get_project(project_name)
        except ValueError:
            raise ValueError(f"Project not found: {project_name}")
        project_id = project.id

        # check if knowledge base exists
        kb = self.get(name, project_id)
        if kb is None:
            # knowledge base does not exist
            if if_exists:
                return
            else:
                raise EntityNotExistsError("Knowledge base does not exist", name)

        # drop table
        vector_db = db.Integration.query.get(kb.vector_database_id)
        if vector_db:
            database_name = vector_db.name
            self.session.datahub.get(database_name).integration_handler.drop_table(
                kb.vector_database_table
            )

        # drop objects if they were created automatically
        if 'vector_storage' in kb.params:
            try:
                self.session.integration_controller.delete(kb.params['vector_storage'])
            except EntityNotExistsError:
                pass
        if 'embedding_model' in kb.params:
            try:
                self.session.model_controller.delete_model(kb.params['embedding_model'], project_name)
            except EntityNotExistsError:
                pass

        # kb exists
        db.session.delete(kb)
        db.session.commit()

    def get(self, name: str, project_id: int) -> db.KnowledgeBase:
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

    def get_table(self, name: str, project_id: int) -> KnowledgeBaseTable:
        """
        Returns kb table object
        :param name: table name
        :param project_id: project id
        :return: kb table object
        """
        kb = self.get(name, project_id)
        if kb is not None:
            return KnowledgeBaseTable(kb, self.session)

    def list(self, project_name: str = None) -> List[dict]:
        """
        List all knowledge bases from the database
        belonging to a project
        """
        project_controller = ProjectController()
        projects = project_controller.get_list()
        if project_name is not None:
            projects = [p for p in projects if p.name == project_name]

        query = (
            db.session.query(db.KnowledgeBase)
            .filter(db.KnowledgeBase.project_id.in_(list([p.id for p in projects])))
        )

        data = []
        project_names = {
            i.id: i.name
            for i in project_controller.get_list()
        }

        for record in query:
            vector_database = record.vector_database
            embedding_model = record.embedding_model

            data.append({
                'id': record.id,
                'name': record.name,
                'project_id': record.project_id,
                'project_name': project_names[record.project_id],
                'embedding_model': embedding_model.name if embedding_model is not None else None,
                'vector_database': None if vector_database is None else vector_database.name,
                'vector_database_table': record.vector_database_table,
                'params': record.params
            })

        return data

    def update(self, name: str, project_id: int, **kwargs) -> db.KnowledgeBase:
        """
        Update a knowledge base record
        """
        raise NotImplementedError()

from typing import List

import mindsdb.interfaces.storage.db as db


class KnowledgeBaseController:
    def __init__(self) -> None:
        ...

    def add(
        self,
        name: str,
        project_id: str,
        vector_database_id: str,
        vector_database_table_name: str,
        embedding_model_id: str,
        params: dict,
        if_not_exists: bool = False,
    ) -> int:
        """
        Add a new knowledge base to the database
        """
        # check if knowledge base already exists
        try:
            kb = self.get(name, project_id)
        except ValueError:
            # knowledge base does not exist
            kb = db.KnowledgeBase(
                name=name,
                project_id=project_id,
                vector_database_id=vector_database_id,
                vector_database_table=vector_database_table_name,
                embedding_model_id=embedding_model_id,
                params=params,
            )
            db.session.add(kb)
            db.session.commit()
            return kb.id

        # kb already exists
        if if_not_exists:
            return kb.id
        else:
            raise Exception(f"Knowledge base already exists: {name}")

    def delete(self, name: str, project_id: str, if_exists: bool = False) -> None:
        """
        Delete a knowledge base from the database
        """
        # check if knowledge base exists
        try:
            kb = self.get(name, project_id)
        except ValueError:
            # knowledge base does not exist
            if if_exists:
                return
            else:
                raise Exception(f"Knowledge base does not exist: {name}")

        # kb exists
        db.session.delete(kb)
        db.session.commit()

    def get(self, name: str, project_id: str) -> db.KnowledgeBase:
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
        if kb is None:
            raise ValueError(f"Knowledge base not found: {name}")
        return kb

    def get_by_id(self, id: str) -> db.KnowledgeBase:
        """
        Get a knowledge base from the database
        by id
        """
        kb = (
            db.session.query(db.KnowledgeBase)
            .filter_by(
                id=id,
            )
            .first()
        )
        if kb is None:
            raise ValueError(f"Knowledge base not found: {id}")
        return kb

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

    def update_by_id(self, id: str, **kwargs) -> db.KnowledgeBase:
        """
        Update a knowledge base from the database
        """
        raise NotImplementedError()

    def list_kb_context_entry(self, session_controller) -> List[dict]:
        """
        List all knowledge base context entries
        """
        # TODO: this is n+1 query, need to optimize
        kbs = db.session.query(
            db.KnowledgeBase,
        ).all()
        kb_context_entries = [
            self.get_kb_context_entry_by_id(kb.id, session_controller) for kb in kbs
        ]
        return kb_context_entries

    def get_kb_context_entry_by_id(self, id: str, session_controller) -> dict:
        """
        Get the knowledge base context entry
        in the format of
        {
            "name": "my_kb",
            "type": "knowledge_base",
            "model": "mindsdb.my_model",
            "storage": "my_chromadb.my_table",
            "search_vector_field": "search_vector",
            "embeddings_field": "embeddings",
            "content_field": "content",
        }
        """
        kb = self.get_by_id(id)
        name = kb.name
        type_ = "knowledge_base"
        search_vector_field = "search_vector"

        # get model
        model_id = kb.embedding_model_id
        # get the input columns
        model = (
            db.session.query(db.Predictor)
            .filter_by(
                id=model_id,
            )
            .first()
        )

        assert model is not None, f"Model not found: {model_id}"
        model_project = (
            db.session.query(db.Project)
            .filter_by(
                id=model.project_id,
            )
            .first()
        )

        assert model_project is not None, f"Project not found: {model.project_id}"
        model_name = f"{model_project.name}.{model.name}"

        # get the output columns
        # describe the model
        args_df = session_controller.model_controller.describe_model(
            session=session_controller,
            project_name=model_project.name,
            model_name=model.name,
            attribute="args",
        )
        args_df.set_index("key", inplace=True)
        embeddings_field = args_df.loc["target", "value"]
        # TODO: assuming there is only one input column
        # need to support multiple input columns
        content_field = args_df.loc["input_columns", "value"][0]

        # get the vector database name + table
        database_id = kb.vector_database_id
        table_name = kb.vector_database_table
        # get the database object
        database = (
            db.session.query(db.Integration)
            .filter_by(
                id=database_id,
            )
            .first()
        )

        assert database is not None, f"Database not found: {database_id}"

        storage = f"{database.name}.{table_name}"
        return {
            "name": name,
            "type": type_,
            "model": model_name,
            "storage": storage,
            "search_vector_field": search_vector_field,
            "embeddings_field": embeddings_field,
            "content_field": content_field,
        }

from mindsdb_sql_parser.ast import Select, BinaryOperation, Identifier, Constant, Star
from mindsdb.integrations.libs.vectordatabase_handler import TableField

from typing import Any, List, Optional

from mindsdb.integrations.utilities.rag.loaders.vector_store_loader.base_vector_store import VectorStore
from mindsdb.interfaces.knowledge_base.preprocessing.document_types import SimpleDocument


class MDBVectorStore(VectorStore):
    def __init__(self, kb_table) -> None:
        self.kb_table = kb_table

    @property
    def embeddings(self) -> Optional[Any]:
        return None

    def similarity_search(
        self,
        query: str,
        k: int = 4,
        **kwargs: Any,
    ) -> List[SimpleDocument]:
        query = Select(
            targets=[Star()],
            where=BinaryOperation(op="=", args=[Identifier(TableField.CONTENT.value), Constant(query)]),
            limit=Constant(k),
        )

        df = self.kb_table.select_query(query)

        docs = []
        for _, row in df.iterrows():
            metadata = row[TableField.METADATA.value]
            if metadata is None:
                metadata = {}
            docs.append(SimpleDocument(page_content=row[TableField.CONTENT.value], metadata=metadata))

        return docs

    def add_texts(self, *args, **kwargs) -> List[str]:
        raise NotImplementedError

    @classmethod
    def from_texts(self, *args, **kwargs):
        raise NotImplementedError

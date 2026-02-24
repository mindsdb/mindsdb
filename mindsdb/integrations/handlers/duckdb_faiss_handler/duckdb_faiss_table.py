
from pathlib import Path
from typing import List

import pandas as pd
import orjson
import duckdb
from mindsdb_sql_parser.ast import (
    Select,
    Delete,
    Identifier,
    BinaryOperation,
    Constant,
    NullConstant,
    Star,
    Tuple as AstTuple,
    Function,
    TypeCast,
)


from mindsdb.integrations.libs.vectordatabase_handler import (
    FilterCondition,
    VectorStoreHandler,
    FilterOperator,
)
from mindsdb.integrations.libs.keyword_search_base import KeywordSearchBase
from mindsdb.integrations.utilities.sql_utils import KeywordSearchArgs

from mindsdb.utilities import log
from mindsdb.utilities.render.sqlalchemy_render import SqlalchemyRender

from .faiss_index import FaissIVFIndex

logger = log.getLogger(__name__)



class DuckDBFaissTable:
    def __init__(self, table_name: str, table_dir: Path, handler: "DuckDBFaissHandler"):
        self.table_name = table_name
        self.handler = handler
        self.connection: duckdb.DuckDBPyConnection | None = None
        self.faiss_index: FaissIVFIndex | None = None
        self.table_dir = table_dir
        self.is_kw_index_enabled = False
        self.cache_required = False

    def open(self) -> "DuckDBFaissTable":

        duckdb_path = self.table_dir / "duckdb.db"
        self.connection = duckdb.connect(str(duckdb_path))
        self.faiss_index = FaissIVFIndex(str(self.table_dir), self.handler.connection_data)

        self.cache_required = self.faiss_index.lock_required

        # check keyword index
        with self.connection.cursor() as cur:
            # check index exists
            df = cur.execute(
                "SELECT * FROM information_schema.schemata WHERE schema_name = 'fts_main_meta_data'"
            ).fetchdf()
            if len(df) > 0:
                self.is_kw_index_enabled = True

        return self

    def close(self) -> None:
        self.faiss_index.close()
        self.connection.close()

    def _create_kw_index(self):
        with self.connection.cursor() as cur:
            cur.execute("PRAGMA create_fts_index('meta_data', 'id', 'content')")

    def _drop_kw_index(self):
        with self.connection.cursor() as cur:
            cur.execute("pragma drop_fts_index('meta_data')")

    def _sync(self, dump_faiss=True):
        if dump_faiss:
            self.faiss_index.dump()

        if self.handler._use_handler_storage:
            self.handler.handler_storage.folder_sync(self.table_name)

    def create_index(self, type: str = "ivf_file", nlist: int = None, train_count: int = None):
        if type not in ("ivf", "ivf_file"):
            raise NotImplementedError("Only ivf or ivf_file indexes are supported")
        self.faiss_index.create_index(type, nlist=nlist, train_count=train_count)
        # index was already saved. don't dump it twice
        self._sync(dump_faiss=False)

    def insert(self, data: pd.DataFrame):
        """Insert data into both DuckDB and Faiss."""

        if self.is_kw_index_enabled:
            # drop index, it will be created before a first keyword search
            self._drop_kw_index()

        with self.connection.cursor() as cur:
            df_ids = cur.execute("""
                insert into meta_data (id, content, metadata) (
                    select id, content, metadata from data
                )
                RETURNING faiss_id, id
            """).fetchdf()

        data = data.merge(df_ids, on="id")

        vectors = data["embeddings"]
        ids = data["faiss_id"]

        self.faiss_index.insert(list(vectors), list(ids))
        self._sync()

    def select(
        self,
        conditions: List[FilterCondition] = None,
        offset: int = None,
        limit: int = None,
    ) -> pd.DataFrame:
        """Select data with hybrid search logic."""

        vector_filter = None
        meta_filters = []
        if conditions is None:
            conditions = []
        for condition in conditions:
            if condition.column == "embeddings":
                vector_filter = condition
            else:
                meta_filters.append(condition)

        if vector_filter is None:
            # If only metadata in filter:
            # query duckdb only
            return self._select_from_metadata(meta_filters=meta_filters, limit=limit).drop("faiss_id", axis=1)

        # vector_filter is not None
        if not meta_filters:
            # If only content in filter: query faiss and attach to metadata
            return self._select_with_vector(vector_filter=vector_filter, limit=limit)

        """
        If metadata + content:
        Query faiss, use limit = 1000
        Query duckdb with `id in (...)` 
        If count of results is less than input LIMIT value
        Repeat the search with increased limit value
        Limit value for step = 1000 * 5^i  (1000, 2000, 25000, 125000 …)
        """

        df = pd.DataFrame()

        total_size = self.get_total_size()

        for i in range(10):
            batch_size = 1000 * 5 ** i

            # TODO implement reverse search:
            #   if batch_size > 25% of db: search metadata first and then in faiss by list of ids

            df = self._select_with_vector(vector_filter=vector_filter, meta_filters=meta_filters, limit=batch_size)
            if batch_size >= total_size or len(df) >= limit:
                break

        return df[:limit]

    def keyword_select(
        self,
        conditions: List[FilterCondition] = None,
        offset: int = None,
        limit: int = None,
        keyword_search_args: KeywordSearchArgs = None,
    ) -> pd.DataFrame:
        if not self.is_kw_index_enabled:
            # keyword search is used for first time: create index
            self._create_kw_index()

        with self.connection.cursor() as cur:
            where_clause = self._translate_filters(conditions)

            score = Function(
                namespace="fts_main_meta_data",
                op="match_bm25",
                args=[
                    Identifier("id"),
                    Constant(keyword_search_args.query),
                    BinaryOperation(op=":=", args=[Identifier("fields"), Constant(keyword_search_args.column)]),
                ],
            )

            no_emtpy_score = BinaryOperation(op="is not", args=[score, NullConstant()])
            if where_clause:
                where_clause = BinaryOperation(op="and", args=[where_clause, no_emtpy_score])
            else:
                where_clause = no_emtpy_score

            query = Select(
                targets=[Star(), BinaryOperation(op="-", args=[Constant(1), score], alias=Identifier("distance"))],
                from_table=Identifier("meta_data"),
                where=where_clause,
            )

            if limit is not None:
                query.limit = Constant(limit)

            if offset is not None:
                query.offset = Constant(offset)

            sql = self.handler.renderer.get_string(query, with_failback=True)
            cur.execute(sql)
            df = cur.fetchdf()
            df["metadata"] = df["metadata"].apply(orjson.loads)
            return df

    def delete(self, conditions: List[FilterCondition] = None):
        """Delete data from both DuckDB and Faiss."""
        with self.connection.cursor() as cur:
            where_clause = self._translate_filters(conditions)

            query = Select(targets=[Identifier("faiss_id")], from_table=Identifier("meta_data"), where=where_clause)
            cur.execute(self.handler.renderer.get_string(query, with_failback=True))
            df = cur.fetchdf()
            ids = list(df["faiss_id"])

            self.faiss_index.delete_ids(ids)

            query = Delete(table=Identifier("meta_data"), where=where_clause)
            cur.execute(self.handler.renderer.get_string(query, with_failback=True))

        self._sync()

    def get_dimension(self) -> int:
        if self.faiss_index and self.faiss_index.index is not None:
            return self.faiss_index.dim

    def get_total_size(self):
        with self.connection.cursor() as cur:
            cur.execute("select count(1) size from meta_data")
            df = cur.fetchdf()
            return df["size"].iloc[0]

    def _select_with_vector(self, vector_filter: FilterCondition, meta_filters=None, limit=None) -> pd.DataFrame:
        embedding = vector_filter.value
        if isinstance(embedding, str):
            embedding = orjson.loads(embedding)

        distances, faiss_ids = self.faiss_index.search(embedding, limit or 100)

        # Fetch full data from DuckDB
        if len(faiss_ids) > 0:
            # ids = [str(idx) for idx in faiss_ids]
            meta_df = self._select_from_metadata(faiss_ids=faiss_ids, meta_filters=meta_filters)
            vector_df = pd.DataFrame({"faiss_id": faiss_ids, "distance": distances})
            return vector_df.merge(meta_df, on="faiss_id").drop("faiss_id", axis=1).sort_values(by="distance")

        return pd.DataFrame([], columns=["id", "content", "metadata", "distance"])


    def _select_from_metadata(self, faiss_ids=None, meta_filters=None, limit=None):
        query = Select(
            targets=[Star()],
            from_table=Identifier("meta_data"),
        )

        where_clause = self._translate_filters(meta_filters)

        if faiss_ids:
            # TODO what if ids list is too long - split search into batches
            in_filter = BinaryOperation(
                op="IN", args=[Identifier("faiss_id"), AstTuple([Constant(i) for i in faiss_ids])]
            )
            # split into chunks
            chunk_size = 10000
            if len(faiss_ids) > chunk_size:
                dfs = []
                chunk = 0
                total = 0
                while chunk * chunk_size < len(faiss_ids):
                    # create results with partition
                    ids = faiss_ids[chunk * chunk_size : (chunk + 1) * chunk_size]
                    chunk += 1
                    df = self._select_from_metadata(faiss_ids=ids, meta_filters=meta_filters, limit=limit)
                    total += len(df)
                    if limit is not None and limit <= total:
                        # cut the extra from the end
                        df = df[: -(total - limit)]
                        dfs.append(df)
                        break
                    if len(df) > 0:
                        dfs.append(df)
                if len(dfs) == 0:
                    return pd.DataFrame([], columns=["faiss_id", "id", "content", "metadata"])
                return pd.concat(dfs)

            if where_clause is None:
                where_clause = in_filter
            else:
                where_clause = BinaryOperation(op="AND", args=[where_clause, in_filter])

        if limit is not None:
            query.limit = Constant(limit)

        query.where = where_clause

        with self.connection.cursor() as cur:
            sql = self.handler.renderer.get_string(query, with_failback=True)
            cur.execute(sql)
            df = cur.fetchdf()
            df["metadata"] = df["metadata"].apply(orjson.loads)
            return df


    def _translate_filters(self, meta_filters):
        if not meta_filters:
            return None

        where_clause = None
        for item in meta_filters:
            parts = item.column.split(".")
            key = Identifier(parts[0])

            # converts 'col.el1.el2' to col->'el1'->>'el2'
            if len(parts) > 1:
                # intermediate elements
                for el in parts[1:-1]:
                    key = BinaryOperation(op="->", args=[key, Constant(el)])

                # last element
                key = BinaryOperation(op="->>", args=[key, Constant(parts[-1])])

            is_orig_id = item.column == "metadata._original_doc_id"

            type_cast = None
            value = item.value

            if isinstance(value, list) and len(value) > 0 and item.op in (FilterOperator.IN, FilterOperator.NOT_IN):
                if is_orig_id:
                    # convert to str
                    item.value = [str(i) for i in value]
                value = item.value[0]
            elif is_orig_id:
                if not isinstance(value, str):
                    value = item.value = str(item.value)

            if isinstance(value, int):
                type_cast = "int"
            elif isinstance(value, float):
                type_cast = "float"

            if type_cast is not None:
                key = TypeCast(type_cast, key)

            if item.op in (FilterOperator.NOT_IN, FilterOperator.IN):
                values = [Constant(i) for i in item.value]
                value = AstTuple(values)
            else:
                value = Constant(item.value)

            condition = BinaryOperation(op=item.op.value, args=[key, value])

            if where_clause is None:
                where_clause = condition
            else:
                where_clause = BinaryOperation(op="AND", args=[where_clause, condition])
        return where_clause
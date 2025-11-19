import os
from typing import Iterable, List
import numpy as np

import portalocker

import faiss  # faiss or faiss-gpu
from pydantic import BaseModel


def _normalize_rows(x: np.ndarray) -> np.ndarray:
    norms = np.linalg.norm(x, axis=1, keepdims=True) + 1e-12
    return x / norms


class FaissParams(BaseModel):
    metric: str | None = "cosine"
    use_gpu: bool | None = False
    nlist: int | None = 1024
    nprobe: int | None = 32
    hnsw_m: int | None = 32
    hnsw_ef_search: int | None = 64


class FaissIndex:
    def __init__(self, path: str, config: dict):
        self._normalize_vectors = False

        self.config = FaissParams(**config)

        metric = self.config.metric
        if metric == "cosine":
            self._normalize_vectors = True
            self.metric = faiss.METRIC_INNER_PRODUCT
        elif metric == "ip":
            self.metric = faiss.METRIC_INNER_PRODUCT
        elif metric == "l1":
            self.metric = faiss.METRIC_L1
        elif metric == "l2":
            self.metric = faiss.METRIC_L2
        else:
            raise ValueError(f"Unknown metric: {metric}")

        self.path = path

        self.index = None
        self.index_fd = None
        if os.path.exists(self.path):
            self.load_index()

    def load_index(self):
        # TODO check RAM
        self.index_fd = open(self.path, "rb")
        try:
            portalocker.lock(self.index_fd, portalocker.LOCK_EX | portalocker.LOCK_NB)
        except ValueError:
            raise ValueError(f'Index is already used: {self.path}')

        self.index = faiss.read_index(self.path)
        self.dim = self.index.d

    def close(self):
        if self.index_fd is not None:
            self.index_fd.close()
        self.index = None

    def _build_index(self):
        # TODO option to create hnsw

        index = faiss.IndexFlat(self.dim, self.metric)
        index = faiss.IndexIDMap(index)

        if self.config.use_gpu:
            try:
                index = faiss.index_cpu_to_all_gpus(index)
            except Exception:
                pass

        self.index = index

    def insert(
        self,
        vectors: Iterable[Iterable[float]],
        ids: Iterable[float],
    ) -> None:
        if len(vectors) == 0:
            return

        # TODO check RAM usage

        vectors = np.array(vectors)
        ids = np.array(ids)

        if self.index is None:
            # this if the first insert, detect dimension
            self.dim = vectors.shape[1]

            self._build_index()

        if vectors.shape[1] != self.dim:
            raise ValueError(f"Dimension mismatch: expected {self.dim}, got {vectors.shape[1]}")

        if self._normalize_vectors:
            vectors = _normalize_rows(vectors)

        self.index.add_with_ids(vectors, ids)

    def delete_ids(self, ids: List[int]) -> None:
        """Mark IDs as deleted for filtering in searches."""
        ids = np.array(ids)
        if self.index:
            self.index.remove_ids(ids)

    def dump(self):
        # TODO to not save it every time for big files?
        #  use two indexes: main and temporal
        #  temporal is Flat and stores data that wasn't moved into main, and have limit
        if self.index:
            faiss.write_index(self.index, self.path)

    def apply_index(self):
        # TODO convert into IndexIVFFlat or IndexHNSWFlat
        ...

    def drop(self):
        if os.path.exists(self.path):
            os.remove(self.path)

    def search(
        self,
        query: Iterable[Iterable[float]],
        limit: int = 10,
        # allowed_ids: Optional[Sequence[int]] = None,
    ):
        if self.index is None:
            return [], []

        queries = np.array([query])

        if self._normalize_vectors:
            queries = _normalize_rows(queries)

        ds, ids = self.index.search(queries, limit)

        list_id = [i for i in ids[0] if i != -1]
        list_distances = [1 - d for d in ds[0][: len(list_id)]]

        return list_distances, list_id

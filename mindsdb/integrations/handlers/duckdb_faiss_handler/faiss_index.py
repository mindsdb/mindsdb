import os
import json
from typing import Iterable, List, Optional, Sequence, Tuple, Any, Union
import numpy as np
import faiss  # faiss or faiss-gpu


def _as_np(x: Union[np.ndarray, Iterable[Iterable[float]]]) -> np.ndarray:
    arr = np.ascontiguousarray(np.array(x, dtype="float32"))
    if arr.ndim == 1:
        arr = arr.reshape(1, -1)
    return arr


def _normalize_rows(x: np.ndarray) -> np.ndarray:
    norms = np.linalg.norm(x, axis=1, keepdims=True) + 1e-12
    return x / norms


class FaissIndexWithFilter:
    def __init__(
        self,
        dim: int = None,
        metric: str = "cosine",      # "cosine" or "l2"
        backend: str = "hnsw",        # "ivf", "flat", "hnsw"
        use_gpu: bool = False,
        nlist: int = 1024,
        nprobe: int = 32,
        train_threshold: int = 5000,
        hnsw_m: int = 32,
        hnsw_ef_search: int = 64,
        hnsw_ef_construction: int = 200,
        **kwargs
    ):
        assert metric in {"cosine", "l2"}
        assert backend in {"ivf", "flat", "hnsw"}

        self.dim = dim
        self.metric = metric
        self.backend = backend
        self.use_gpu = use_gpu

        self.nlist = nlist
        self.nprobe = nprobe
        self.train_threshold = train_threshold

        self.hnsw_m = hnsw_m
        self.hnsw_ef_search = hnsw_ef_search
        self.hnsw_ef_construction = hnsw_ef_construction

        self._index: Optional[faiss.Index] = None
        self._is_trained = False

        self._buf_vectors: List[np.ndarray] = []
        self._buf_ids: List[int] = []

        self._next_id = 0
        self._meta: dict[int, Any] = {}
        self._index_path = None
        self._meta_path = None
        
        # Track deleted IDs for filtering
        self._deleted_ids: set = set()

    def _build_index(self):
        if self.metric == "cosine":
            faiss_metric = faiss.METRIC_INNER_PRODUCT
        else:
            faiss_metric = faiss.METRIC_L2

        if self.backend == "flat":
            base = faiss.IndexFlat(self.dim, faiss_metric)
        elif self.backend == "hnsw":
            base = faiss.IndexHNSWFlat(self.dim, self.hnsw_m, faiss_metric)
            base.hnsw.efSearch = self.hnsw_ef_search
            base.hnsw.efConstruction = self.hnsw_ef_construction
        else:  # "ivf"
            quantizer = faiss.IndexFlat(self.dim, faiss_metric)
            base = faiss.IndexIVFFlat(quantizer, self.dim, self.nlist, faiss_metric)
            base.nprobe = self.nprobe

        if self.use_gpu:
            try:
                base = faiss.index_cpu_to_all_gpus(base)
            except Exception:
                pass

        self._index = base
        self._is_trained = (self.backend != "ivf")

    def add(
        self,
        vectors: Union[np.ndarray, Iterable[Iterable[float]]],
        ids: Optional[Sequence[int]] = None,
        batch_size: int = 65536,
    ) -> None:
        X = _as_np(vectors)

        if self._index is None:
            # first insert
            self.dim = X.shape[1]
            self._build_index()

        if X.shape[1] != self.dim:
            raise ValueError(f"Dimension mismatch: expected {self.dim}, got {X.shape[1]}")

        if self.metric == "cosine":
            X = _normalize_rows(X)

        N = X.shape[0]
        # if ids is None:
        ids = [self._alloc_id() for _ in range(N)]
        # else:
        #     if len(ids) != N:
        #         raise ValueError("Length of ids must match number of vectors")

        if self.backend == "ivf" and not self._is_trained:
            # buffer until we reach threshold
            self._buf_vectors.append(X)
            self._buf_ids.extend(ids)
            total_buf = sum(buf.shape[0] for buf in self._buf_vectors)
            # if total_buf >= self.train_threshold:
            self._train_ivf_and_flush(batch_size=batch_size)
        else:
            self._batched_add(X, ids, batch_size=batch_size)

    def _train_ivf_and_flush(self, batch_size: int):
        assert self.backend == "ivf"
        assert self._index is not None

        Xbuf = np.vstack(self._buf_vectors) if self._buf_vectors else np.empty((0, self.dim), dtype="float32")
        ids = np.array(self._buf_ids, dtype="int64") if self._buf_ids else np.empty((0,), dtype="int64")

        if not self._is_trained:
            if self.use_gpu:
                try:
                    cpu_idx = faiss.index_gpu_to_cpu(self._index)
                except Exception:
                    cpu_idx = self._index
                if not cpu_idx.is_trained:
                    cpu_idx.train(Xbuf)
                self._index = faiss.index_cpu_to_all_gpus(cpu_idx)
            else:
                if not self._index.is_trained:
                    self._index.train(Xbuf)
            self._is_trained = True

        self._batched_add(Xbuf, ids.tolist(), batch_size=batch_size)
        self._buf_vectors.clear()
        self._buf_ids.clear()

    def _batched_add(self, X: np.ndarray, ids: Sequence[int], batch_size: int):
        assert self._index is not None
        ids_np = np.asarray(ids, dtype="int64")
        n = X.shape[0]
        for i0 in range(0, n, batch_size):
            i1 = min(n, i0 + batch_size)
            self._index.add_with_ids(X[i0:i1], ids_np[i0:i1])

    def _alloc_id(self) -> int:
        _id = self._next_id
        self._next_id += 1
        return _id

    @property
    def ntotal(self) -> int:
        return int(self._index.ntotal) if self._index is not None else 0

    def delete_ids(self, ids: List[int]) -> None:
        """Mark IDs as deleted for filtering in searches."""
        self._deleted_ids.update(ids)

    def drop(self):
        if os.path.exists(self._index_path):
            os.remove(self._index_path)
        if os.path.exists(self._meta_path):
            os.remove(self._meta_path)

    def save(self):
        if self._index is None:
            raise RuntimeError("No index to save")
        faiss.write_index(self._index, self._index_path)
        side = {
            "dim": self.dim,
            "metric": self.metric,
            "backend": self.backend,
            "use_gpu": self.use_gpu,
            "nlist": self.nlist,
            "nprobe": self.nprobe,
            "train_threshold": self.train_threshold,
            "hnsw_m": self.hnsw_m,
            "hnsw_ef_search": self.hnsw_ef_search,
            "hnsw_ef_construction": self.hnsw_ef_construction,
            "is_trained": self._is_trained,
            "next_id": self._next_id,
            "meta": self._meta,
            "deleted_ids": list(self._deleted_ids),
        }
        with open(self._meta_path, "w") as f:
            json.dump(side, f)

    @classmethod
    def load(cls, path: str):
        meta = {}
        meta_path = os.path.join(path, "m.meta")
        index_path = os.path.join(path, "i.idx")

        if os.path.exists(meta_path):
            with open(meta_path, "r") as f:
                meta = json.load(f)
        obj = cls(**meta)
        obj._index_path = index_path
        obj._meta_path = meta_path
        # obj._is_trained = side["is_trained"]
        # obj._next_id = side["next_id"]
        # obj._meta = {int(k): v for k, v in side.get("meta", {}).items()}
        # obj._deleted_ids = set(side.get("deleted_ids", []))

        if os.path.exists(index_path):
            obj._index = faiss.read_index(index_path)

            if obj.backend == "hnsw":
                h = faiss.downcast_index(obj._index)
                if hasattr(h, "hnsw"):
                    h.hnsw.efSearch = obj.hnsw_ef_search

        return obj

    def search(
        self,
        queries: Union[np.ndarray, Iterable[Iterable[float]]],
        k: int = 10,
        allowed_ids: Optional[Sequence[int]] = None,
        return_metadata: bool = True,
    ) -> Tuple[np.ndarray, np.ndarray]:
        """
        Search with optional filtering by allowed_ids.
        If allowed_ids is provided, we first try to use native FAISS filtering
        (via IDSelector & SearchParameters). If that fails or is unsupported,
        we fall back to post-filtering.
        """
        if self._index is None:
            return [], []

        Q = _as_np(queries)
        if Q.shape[1] != self.dim:
            raise ValueError(f"Query dimension mismatch: {Q.shape[1]} vs {self.dim}")
        if self.metric == "cosine":
            Q = _normalize_rows(Q)

        use_native = False
        D = None
        I = None

        # Combine allowed_ids with non-deleted IDs
        if allowed_ids is not None:
            allowed_ids = set(allowed_ids) - self._deleted_ids
        else:
            allowed_ids = None

        # Try to use native filtering if possible
        if allowed_ids is not None:
            try:
                # Build an ID selector
                arr = np.array(list(allowed_ids), dtype="int64")
                sel = faiss.IDSelectorBatch(len(arr), arr)
                # Use a SearchParameters object
                params = faiss.SearchParametersIVF()
                params.sel = sel
                params.nprobe = self.nprobe
                D, I = self._index.search(Q, k, params)
                use_native = True
            except Exception as e:
                # fallback path
                # print("Warning: native ID-filter search failed:", e)
                use_native = False

        if not use_native:
            # fallback: normal search then post-filter
            D, I = self._index.search(Q, k * 3)  # oversample
            if allowed_ids is not None:
                allowed = set(allowed_ids)
                new_D = []
                new_I = []
                for drow, irow in zip(D, I):
                    filtered = [(d, int(i)) for d, i in zip(drow, irow) if i in allowed]
                    # pad if too few or trim
                    if len(filtered) < k:
                        filtered.extend([(0.0, -1)] * (k - len(filtered)))
                    filtered = filtered[:k]
                    d_new, i_new = zip(*filtered)
                    new_D.append(d_new)
                    new_I.append(i_new)
                D = np.array(new_D, dtype="float32")
                I = np.array(new_I, dtype="int64")
            else:
                # Filter out deleted IDs
                new_D = []
                new_I = []
                for drow, irow in zip(D, I):
                    filtered = [(d, int(i)) for d, i in zip(drow, irow) if int(i) not in self._deleted_ids]
                    # pad if too few or trim
                    if len(filtered) < k:
                        filtered.extend([(0.0, -1)] * (k - len(filtered)))
                    filtered = filtered[:k]
                    d_new, i_new = zip(*filtered)
                    new_D.append(d_new)
                    new_I.append(i_new)
                D = np.array(new_D, dtype="float32")
                I = np.array(new_I, dtype="int64")

        metas = None
        if return_metadata:
            metas = [[self._meta.get(int(i)) if int(i) != -1 else None for i in row] for row in I]

        return D, I

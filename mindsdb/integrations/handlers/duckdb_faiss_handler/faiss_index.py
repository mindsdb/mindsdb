import os
import pathlib
from typing import Iterable, List, Callable
import numpy as np
import psutil
from pathlib import Path

import portalocker

import faiss  # faiss or faiss-gpu
from faiss.contrib.ondisk import merge_ondisk
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

        self._since_ram_checked = 0

        self.index = None
        self.index_type = "flat"
        self.dim = None
        self.index_fd = None
        if os.path.exists(self.path):
            self._load_index()

    def _lock_index(self):
        if os.name != "nt":
            self.index_fd = open(self.path, "rb")
            try:
                portalocker.lock(self.index_fd, portalocker.LOCK_EX | portalocker.LOCK_NB)
            except portalocker.exceptions.AlreadyLocked:
                raise ValueError(f"Index is already used: {self.path}")

    def _load_index(self):
        # check RAM
        index_size = os.path.getsize(self.path)
        # according to tests faiss index occupies ~ the same amount of RAM as file size
        # add 10% and 1Gb to it, check only if index > 1Gb
        _1gb = 1024**3
        required_ram = index_size * 1.1 + _1gb
        available_ram = psutil.virtual_memory().available
        if required_ram > _1gb and available_ram < required_ram:
            to_free_gb = round((required_ram - available_ram) / _1gb, 2)
            raise ValueError(f"Unable load FAISS index into RAM, free up al least : {to_free_gb} Gb")

        self._lock_index()

        self.index = faiss.read_index(self.path)
        self.dim = self.index.d

        index = self.index
        if hasattr(index, "index"):
            index = faiss.downcast_index(index.index)
        if isinstance(index, faiss.IndexIVFFlat):
            self.index_type = "ivf"

    def close(self):
        if self.index_fd is not None:
            self.index_fd.close()
        self.index = None

    def _build_flat_index(self):
        # TODO option to create hnsw

        index = faiss.IndexFlat(self.dim, self.metric)
        index = faiss.IndexIDMap(index)

        if self.config.use_gpu:
            try:
                index = faiss.index_cpu_to_all_gpus(index)
            except Exception:
                pass

        self.index = index

    def _check_ram_usage(self, count_vectors, index_type: str = "flat", m=32, nlist=4096):
        self._since_ram_checked += count_vectors

        # check after every 10k vectors
        if self._since_ram_checked < 10000:
            return

        match index_type:
            case "flat":
                required = self.dim * 4 * count_vectors
            case "hnsw":
                required = (self.dim * 4 + m * 2 * 4) * count_vectors
            case "ivf":
                required = (self.dim * 4 + 8) * count_vectors + self.dim * 4 * nlist
            case _:
                raise ValueError(f"Unknown index type: {index_type}")

        # check RAM usage
        # keep extra 1Gb
        available = psutil.virtual_memory().available - 1 * 1024**3

        if available < required:
            raise ValueError("Unable insert records, not enough RAM")

        self._since_ram_checked = 0

    def insert(
        self,
        vectors: Iterable[Iterable[float]],
        ids: Iterable[int],
    ) -> None:
        if len(vectors) == 0:
            return

        vectors = np.array(vectors)
        ids = np.array(ids)

        if self.index is None:
            # this if the first insert, detect dimension
            self.dim = vectors.shape[1]

            self._build_flat_index()

        self._check_ram_usage(len(vectors), self.index_type)

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

    def drop(self):
        self.close()
        if os.path.exists(self.path):
            os.remove(self.path)

    def search(
        self,
        query: Iterable[float],
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


class FaissIVFIndex(FaissIndex):
    def _dump_vectors(self, index, path: pathlib.Path, batch_size: int = 30000):
        """
        Extract and dump vectors and ids from index. Method is dependent on index type
        """

        if hasattr(index, "id_map"):
            ids = faiss.vector_to_array(index.id_map).astype(np.int64, copy=False)
            inner = index.index

            def get_batch_vectors(start, size):
                return inner.reconstruct_n(start, size).astype(np.float32, copy=False)

            return self._dump_vectors_to_file(ids, path, index.ntotal, batch_size, get_batch_vectors)
        else:
            invlists = index.invlists

            index.set_direct_map_type(faiss.DirectMap.Hashtable)

            ids_list = []
            for list_no in range(index.nlist):
                list_size = invlists.list_size(list_no)
                if list_size == 0:
                    continue

                # Get IDs stored in this inverted list
                id_array = faiss.rev_swig_ptr(invlists.get_ids(list_no), list_size)
                ids_list.append(id_array)

            ids = np.hstack(ids_list).astype(np.int64)

            # to train index first batches will be used. shuffle ids to prevent using the same lists
            # TODO shuffle only part of data?
            np.random.shuffle(ids)

            def get_batch_vectors(start, size):
                ids_batch = ids[start : start + size]
                return index.reconstruct_batch(ids_batch).astype(np.float32, copy=False)

            return self._dump_vectors_to_file(ids, path, index.ntotal, batch_size, get_batch_vectors)

    def _dump_vectors_to_file(
        self,
        ids: np.ndarray,
        path: pathlib.Path,
        ntotal: int,
        batch_size: int,
        get_batch_f: Callable[[int, int], np.ndarray],
    ) -> int:
        """

        Write ids and vectors to memmap files in batches.

        :param ids: vector IDs in the same order as vectors will be dumped.
        :param path: directory to store dumps.
        :param ntotal: total number of vectors.
        :param batch_size: number of vectors per batch file.
        :param get_batch_f: function to get a batch content

        """

        # Write all ids once to a single memmap file
        ids_path = path / "ids.mmap"
        mmap_ids = np.memmap(ids_path, dtype=np.int64, mode="w+", shape=(ntotal,))
        mmap_ids[:] = ids

        batch_num = 0
        while True:
            if ntotal <= 0:
                break

            start = batch_num * batch_size
            size = min(ntotal, batch_size)

            ntotal -= size
            batch_num += 1

            vecs = get_batch_f(start, size)

            vecs_path = path / f"batch_{batch_num:05d}_vecs.mmap"

            # Create memmap for vectors and write
            mmap_vecs = np.memmap(vecs_path, dtype=np.float32, mode="w+", shape=(size, self.dim))
            mmap_vecs[:] = vecs
            mmap_vecs.flush()
            del mmap_vecs

        del mmap_ids
        return batch_num

    def _train_ivf(self, dump_path, train_count, nlist):
        # Accumulate training data up to train_count
        train_left = train_count
        train_chunks = []

        vec_files = self._get_dump_vector_files(dump_path)

        for fname in vec_files:
            fpath = dump_path / fname
            batch_data = np.fromfile(fpath, dtype="float32")
            rows = int(batch_data.shape[0] / self.dim)

            train_chunks.append(batch_data.reshape([rows, self.dim]))

            train_left -= rows
            if train_left <= 0:
                break

        train_data = np.vstack(train_chunks)
        train_data = train_data[:train_count, :]

        quantizer = faiss.IndexFlat(self.dim, self.metric)
        ivf = faiss.IndexIVFFlat(quantizer, self.dim, nlist, self.metric)

        ivf.train(train_data)
        return ivf

    def _get_dump_vector_files(self, dump_path):
        # Collect vector batch files and sort by batch index
        vec_files = [f for f in os.listdir(dump_path) if f.startswith("batch_")]
        if not vec_files:
            raise FileNotFoundError(f"No vector batch memmaps found in {dump_path}")

        vec_files.sort()
        return vec_files

    def _create_ivf_index(self, path, train_count, nlist):
        """
        Build an in-memory IVF index

        :param path: Directory containing memmap files
        :param train_count: Number of vectors to use for training
        :param nlist: number of clusters for IVF
        """

        # Load ids
        ids_path = path / "ids.mmap"
        if not os.path.exists(ids_path):
            raise FileNotFoundError(f"Missing ids memmap: {ids_path}")
        ids = np.fromfile(ids_path, dtype="int64")

        ivf = self._train_ivf(path, nlist=nlist, train_count=train_count)

        vec_files = self._get_dump_vector_files(path)

        # load data
        start = 0
        for fname in vec_files:
            fpath = path / fname

            batch_data = np.fromfile(fpath, dtype="float32")
            rows = int(batch_data.shape[0] / self.dim)

            batch_vectors = batch_data.reshape([rows, self.dim])

            ids_batch = np.asarray(ids[start : start + rows])
            ivf.add_with_ids(batch_vectors, ids_batch)
            start += rows

        return ivf

    def _create_ivf_file_index(self, path, train_count, nlist):
        """Build an IVF on disk index"""

        index_path = path.parent
        trained_index = self._train_ivf(path, train_count=train_count, nlist=nlist)
        # store trained index
        trained_path = str(index_path / "faiss_index.trained")
        faiss.write_index(trained_index, trained_path)

        ids_path = path / "ids.mmap"
        if not os.path.exists(ids_path):
            raise FileNotFoundError(f"Missing ids memmap: {ids_path}")
        ids = np.fromfile(ids_path, dtype="int64")

        vec_files = self._get_dump_vector_files(path)

        start = 0
        block_fnames = []
        for num, fname in enumerate(vec_files):
            index = faiss.read_index(trained_path)
            fpath = path / fname

            batch_data = np.fromfile(fpath, dtype="float32")
            rows = int(batch_data.shape[0] / self.dim)

            batch_vectors = batch_data.reshape([rows, self.dim])

            ids_batch = np.asarray(ids[start : start + rows])
            index.add_with_ids(batch_vectors, ids_batch)
            block_fname = str(index_path / f"faiss_index_block.{num}")
            block_fnames.append(block_fname)
            faiss.write_index(index, block_fname)
            start += rows

        index = faiss.read_index(trained_path)

        merge_ondisk(index, block_fnames, str(index_path / "faiss_index_merged"))
        os.unlink(trained_path)

        return index

    def create_index(self, index_type, nlist=None, train_count=None):
        """
        Create or recreate IVF index

        :param index_type: options are: 'ivf' (in RAM) or 'ivf_file' (on disk)
        :param nlist: number of inverted lists
        :param train_count: count of vectors to use for training.

        """

        # index might not fit into RAM, extract data to files
        dump_path = Path(self.path).parent / "dump"

        # if self.index_type != 'flat':
        #     raise ValueError('Index was already created')

        # check params, apply defaults
        if nlist is None:
            nlist = self.config.nlist

        if self.index is None:
            ntotal = 0
        else:
            ntotal = self.index.ntotal

        nlist_k = 39
        if train_count is not None:
            if train_count < nlist * nlist_k:
                raise ValueError(f"Train_count can't be less than nlist * {nlist_k} (is {nlist * nlist_k})")
        else:
            # get 10k if possible but not less than nlist * k
            train_count = max(nlist * nlist_k, min(ntotal, 10000))

        if train_count > ntotal:
            raise ValueError(f"Not enough data to create index: {ntotal}, at least {train_count} records are required")

        dump_path.mkdir(exist_ok=True)

        # remove old items
        for item in dump_path.iterdir():
            item.unlink()

        self._dump_vectors(self.index, dump_path)

        # unload flat index from RAM
        self.close()

        # clean index dir
        for item in dump_path.parent.iterdir():
            if not item.is_dir():
                item.unlink()

        # create ivf index
        if index_type == "ivf":
            ivf_index = self._create_ivf_index(dump_path, train_count=train_count, nlist=nlist)

        elif index_type == "ivf_file":
            ivf_index = self._create_ivf_file_index(dump_path, train_count=train_count, nlist=nlist)
        else:
            raise ValueError(f"Unknown index type: {index_type}")

        self.index = ivf_index
        self.index_type = "ivf"
        self.dump()
        self._lock_index()

        # remove unused items
        for item in dump_path.iterdir():
            item.unlink()

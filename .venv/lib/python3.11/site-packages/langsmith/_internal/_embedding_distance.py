from __future__ import annotations

import logging
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    List,
    Literal,
    Optional,
    Sequence,
    Union,
)

from typing_extensions import TypedDict

if TYPE_CHECKING:
    import numpy as np  # type: ignore


logger = logging.getLogger(__name__)

Matrix = Union[List[List[float]], List[Any], Any]


def cosine_similarity(X: Matrix, Y: Matrix) -> np.ndarray:
    """Row-wise cosine similarity between two equal-width matrices."""
    import numpy as np

    if len(X) == 0 or len(Y) == 0:
        return np.array([])

    X = np.array(X)
    Y = np.array(Y)
    if X.shape[1] != Y.shape[1]:
        raise ValueError(
            f"Number of columns in X and Y must be the same. X has shape {X.shape} "
            f"and Y has shape {Y.shape}."
        )
    try:
        import simsimd as simd  # type: ignore

        X = np.array(X, dtype=np.float32)
        Y = np.array(Y, dtype=np.float32)
        Z = 1 - simd.cdist(X, Y, metric="cosine")
        if isinstance(Z, float):
            return np.array([Z])
        return np.array(Z)
    except ImportError:
        logger.debug(
            "Unable to import simsimd, defaulting to NumPy implementation. If you want "
            "to use simsimd please install with `pip install simsimd`."
        )
        X_norm = np.linalg.norm(X, axis=1)
        Y_norm = np.linalg.norm(Y, axis=1)
        # Ignore divide by zero errors run time warnings as those are handled below.
        with np.errstate(divide="ignore", invalid="ignore"):
            similarity = np.dot(X, Y.T) / np.outer(X_norm, Y_norm)
        similarity[np.isnan(similarity) | np.isinf(similarity)] = 0.0
        return similarity


def _get_openai_encoder() -> Callable[[Sequence[str]], Sequence[Sequence[float]]]:
    """Get the OpenAI GPT-3 encoder."""
    try:
        from openai import Client
    except ImportError:
        raise ImportError(
            "THe default encoder for the EmbeddingDistance class uses the OpenAI API. "
            "Please either install the openai library with `pip install openai` or "
            "provide a custom encoder function (Callable[[str], Sequence[float]])."
        )

    def encode_text(texts: Sequence[str]) -> Sequence[Sequence[float]]:
        client = Client()
        response = client.embeddings.create(
            input=list(texts), model="text-embedding-3-small"
        )
        return [d.embedding for d in response.data]

    return encode_text


class EmbeddingConfig(TypedDict, total=False):
    encoder: Callable[[List[str]], Sequence[Sequence[float]]]
    metric: Literal["cosine", "euclidean", "manhattan", "chebyshev", "hamming"]


class EmbeddingDistance:
    def __init__(
        self,
        config: Optional[EmbeddingConfig] = None,
    ):
        config = config or {}
        self.distance = config.get("metric") or "cosine"
        self.encoder = config.get("encoder") or _get_openai_encoder()

    def evaluate(
        self,
        prediction: str,
        reference: str,
    ) -> float:
        try:
            import numpy as np
        except ImportError:
            raise ImportError(
                "The EmbeddingDistance class requires NumPy. Please install it with "
                "`pip install numpy`."
            )
        embeddings = self.encoder([prediction, reference])
        vector = np.array(embeddings)
        return self._compute_distance(vector[0], vector[1]).item()

    def _compute_distance(self, a: np.ndarray, b: np.ndarray) -> np.floating:
        if self.distance == "cosine":
            return self._cosine_distance(a, b)  # type: ignore
        elif self.distance == "euclidean":
            return self._euclidean_distance(a, b)
        elif self.distance == "manhattan":
            return self._manhattan_distance(a, b)
        elif self.distance == "chebyshev":
            return self._chebyshev_distance(a, b)
        elif self.distance == "hamming":
            return self._hamming_distance(a, b)
        else:
            raise ValueError(f"Invalid distance metric: {self.distance}")

    @staticmethod
    def _cosine_distance(a: np.ndarray, b: np.ndarray) -> np.ndarray:
        """Compute the cosine distance between two vectors.

        Args:
            a (np.ndarray): The first vector.
            b (np.ndarray): The second vector.

        Returns:
            np.ndarray: The cosine distance.
        """
        return 1.0 - cosine_similarity([a], [b])

    @staticmethod
    def _euclidean_distance(a: np.ndarray, b: np.ndarray) -> np.floating:
        """Compute the Euclidean distance between two vectors.

        Args:
            a (np.ndarray): The first vector.
            b (np.ndarray): The second vector.

        Returns:
            np.floating: The Euclidean distance.
        """
        return np.linalg.norm(a - b)

    @staticmethod
    def _manhattan_distance(a: np.ndarray, b: np.ndarray) -> np.floating:
        """Compute the Manhattan distance between two vectors.

        Args:
            a (np.ndarray): The first vector.
            b (np.ndarray): The second vector.

        Returns:
            np.floating: The Manhattan distance.
        """
        return np.sum(np.abs(a - b))

    @staticmethod
    def _chebyshev_distance(a: np.ndarray, b: np.ndarray) -> np.floating:
        """Compute the Chebyshev distance between two vectors.

        Args:
            a (np.ndarray): The first vector.
            b (np.ndarray): The second vector.

        Returns:
            np.floating: The Chebyshev distance.
        """
        return np.max(np.abs(a - b))

    @staticmethod
    def _hamming_distance(a: np.ndarray, b: np.ndarray) -> np.floating:
        """Compute the Hamming distance between two vectors.

        Args:
            a (np.ndarray): The first vector.
            b (np.ndarray): The second vector.

        Returns:
            np.floating: The Hamming distance.
        """
        return np.mean(a != b)

from typing import Any, Callable, Dict, Literal, Optional

from typing_extensions import TypedDict

METRICS = Literal[
    "damerau_levenshtein",
    "levenshtein",
    "jaro",
    "jaro_winkler",
    "hamming",
    "indel",
]


class EditDistanceConfig(TypedDict, total=False):
    metric: METRICS
    normalize_score: bool


class EditDistance:
    def __init__(
        self,
        config: Optional[EditDistanceConfig] = None,
    ):
        config = config or {}
        metric = config.get("metric") or "damerau_levenshtein"
        self.metric = self._get_metric(
            metric, normalize_score=config.get("normalize_score", True)
        )

    def evaluate(
        self,
        prediction: str,
        reference: Optional[str] = None,
    ) -> float:
        return self.metric(prediction, reference)

    @staticmethod
    def _get_metric(distance: str, normalize_score: bool = True) -> Callable:
        try:
            from rapidfuzz import (  # type: ignore[import-not-found]
                distance as rf_distance,
            )
        except ImportError:
            raise ImportError(
                "This operation requires the rapidfuzz library to use."
                "Please install it with `pip install -U rapidfuzz`."
            )

        module_map: Dict[str, Any] = {
            "damerau_levenshtein": rf_distance.DamerauLevenshtein,
            "levenshtein": rf_distance.Levenshtein,
            "jaro": rf_distance.Jaro,
            "jaro_winkler": rf_distance.JaroWinkler,
            "hamming": rf_distance.Hamming,
            "indel": rf_distance.Indel,
        }
        if distance not in module_map:
            raise ValueError(
                f"Invalid distance metric: {distance}"
                f"\nMust be one of: {list(module_map)}"
            )
        module = module_map[distance]
        if normalize_score:
            return module.normalized_distance
        else:
            return module.distance

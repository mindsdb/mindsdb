from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class ExploreDataset(str, Enum):
    LOGS = "logs"


@dataclass(frozen=True)
class ExploreTableRequest:
    dataset: ExploreDataset
    fields: list[str]
    query: str
    limit: int
    sort: str | None
    start: str | None
    end: str | None
    stats_period: str | None
    project_ids: list[int]
    environments: list[str]


@dataclass(frozen=True)
class ExploreTimeseriesRequest:
    dataset: ExploreDataset
    query: str
    y_axis: str
    interval: int
    start: str | None
    end: str | None
    stats_period: str | None
    project_ids: list[int]
    environments: list[str]

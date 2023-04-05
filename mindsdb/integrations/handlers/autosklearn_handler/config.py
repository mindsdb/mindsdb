import sys
from dataclasses import dataclass
from autosklearn.metrics import *
from autosklearn.metrics import Scorer


@dataclass(frozen=True)
class ClassificationConfig:
    time_left_for_this_task: int = 3600
    per_run_time_limit: int = None
    n_jobs: int = None
    metric: Scorer = 'accuracy'

    def __post_init__(self):
        object.__setattr__(self, 'metric', getattr(sys.modules[__name__], self.metric))

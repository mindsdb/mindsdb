import sys
from dataclasses import dataclass
from autosklearn.metrics import Scorer


@dataclass(frozen=True)
class BaseConfig:
    time_left_for_this_task: int = 3600
    per_run_time_limit: int = None
    n_jobs: int = None
    metric: Scorer = 'accuracy'
    ensemble_size: int = None
    ensemble_nbest: int = 50
    initial_configurations_via_metalearning: int = 25
    resampling_strategy: str = 'holdout'

    def __post_init__(self):
        object.__setattr__(self, 'metric', getattr(sys.modules[__name__], self.metric))


@dataclass(frozen=True)
class ClassificationConfig(BaseConfig):
    pass


@dataclass(frozen=True)
class RegressionConfig(BaseConfig):
    pass

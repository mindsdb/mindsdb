import sys
from dataclasses import dataclass
from autosklearn.metrics import *
from autosklearn.metrics import Scorer
from autogluon.core.metrics import Scorer
@dataclass(frozen=True)
class BaseConfig:
    time_left_for_this_task: int = 3600
    time_limit: int = None
    num_cpu: int = 'auto'
    metric: Scorer = 'accuracy'
    def __post_init__(self):
        object.__setattr__(self, 'metric', getattr(sys.modules[__name__], self.metric))


@dataclass(frozen=True)
class ClassificationConfig(BaseConfig):
    pass


@dataclass(frozen=True)
class RegressionConfig(BaseConfig):
    pass

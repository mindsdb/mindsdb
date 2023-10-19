import sys
from dataclasses import dataclass
# from autosklearn.metrics import *
# from autosklearn.metrics import Scorer
from autogluon.core.metrics import *
from autogluon.core.metrics import Scorer
@dataclass(frozen=True)
class BaseConfig:
    time_limit: int = None
    num_cpus: int = 'auto'


@dataclass(frozen=True)
class ClassificationConfig(BaseConfig):
    pass


@dataclass(frozen=True)
class RegressionConfig(BaseConfig):
    pass

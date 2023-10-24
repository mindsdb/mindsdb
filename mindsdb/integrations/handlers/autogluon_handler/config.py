from dataclasses import dataclass


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

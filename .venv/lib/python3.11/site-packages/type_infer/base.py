from typing import Dict
from dataclasses import dataclass
from dataclasses_json import dataclass_json


@dataclass_json
@dataclass
class TypeInformation:
    """
    For a dataset, provides information on columns types, how they're used, and any other potential identifiers.

    ``TypeInformation`` is generated within :py:func:`infer.infer_types`, where small samples of each column are evaluated in a custom framework to understand what kind of data type the model is. The user may override data types, but it is recommended to do so within a JSON-AI config file.

    :param dtypes: For each column's name, the associated data type inferred.
    :param additional_info: Any possible sub-categories or additional descriptive information.
    :param identifiers: Columns within the dataset highly suspected of being identifiers or IDs. These do not contain informatic value, therefore will be ignored in subsequent training/analysis procedures unless manually indicated.
    """ # noqa

    dtypes: Dict[str, str]
    additional_info: Dict[str, object]
    identifiers: Dict[str, str]

    def __init__(self):
        self.dtypes = dict()
        self.additional_info = dict()
        self.identifiers = dict()


class BaseEngine:
    def __init__(self, stable=True):
        self.stable = stable  # whether the engine is stable or not (i.e. experimental)

    def infer(self, df) -> TypeInformation:
        """Given a dataframe, infer the types of each column and return a TypeInformation object."""
        raise NotImplementedError


class ENGINES:
    RULE_BASED = 'rule_based'

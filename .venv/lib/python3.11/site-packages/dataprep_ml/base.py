from typing import Optional
from dataclasses import dataclass
from dataclasses_json import dataclass_json

from type_infer.base import TypeInformation


@dataclass_json
@dataclass
class StatisticalAnalysis:
    """
    The Statistical Analysis data class allows users to consider key descriptors of their data using simple \
    techniques such as histograms, mean and standard deviation, word count, missing values, and any detected bias \
    in the information.

    :param nr_rows: Number of rows (samples) in the dataset
    :param nr_columns: Number of columns (features) in the dataset
    :param df_target_stddev: The standard deviation of the target of the dataset
    :param train_observed_classes:
    :param target_class_distribution:
    :param target_weights: What weight the analysis suggests to assign each class by in the case of classification problems. Note: target_weights in the problem definition overides this
    :param histograms:
    :param buckets:
    :param missing:
    :param distinct:
    :param bias:
    :param avg_words_per_sentence:
    :param positive_domain:
    :param ts_stats:
    """ # noqa

    nr_rows: int
    nr_columns: int
    df_target_stddev: Optional[float]
    train_observed_classes: object  # Union[None, List[str]]
    target_class_distribution: object  # Dict[str, float]
    target_weights: object  # Dict[str, float]
    histograms: object  # Dict[str, Dict[str, List[object]]]
    buckets: object  # Dict[str, Dict[str, List[object]]]
    missing: object
    distinct: object
    bias: object
    avg_words_per_sentence: object
    positive_domain: bool
    ts_stats: dict


@dataclass_json
@dataclass
class DataAnalysis:
    """
    Data Analysis wraps :class: `.StatisticalAnalysis` and :class: `type_infer.base.TypeInformation` together. 
    
    Further details can be seen in their respective documentation references.
    """ # noqa

    statistical_analysis: StatisticalAnalysis
    type_information: TypeInformation

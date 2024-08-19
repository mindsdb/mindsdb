from typing import Dict, Optional
import pandas as pd

from type_infer.base import TypeInformation, ENGINES
from type_infer.rule_based.core import RuleBasedEngine


def infer_types(
        data: pd.DataFrame,
        config: Optional[Dict] = None
) -> TypeInformation:
    """
    Infers the data types of each column of the dataset by analyzing a small sample of
    each column's items.

    Inputs
    ----------
    data : pd.DataFrame
        The input dataset for which we want to infer data type information.
    """
    # Set global defaults if missing
    if config is None:
        config = {'engine': 'rule_based', 'pct_invalid': 2, 'seed': 420, 'mp_cutoff': 1e4}
    elif 'engine' not in config:
        config['engine'] = 'rule_based'

    if 'pct_invalid' not in config:
        config['pct_invalid'] = 2

    if 'seed' not in config:
        config['seed'] = 420

    if config['engine'] == ENGINES.RULE_BASED:
        if 'mp_cutoff' not in config:
            config['mp_cutoff'] = 1e4

        engine = RuleBasedEngine(config)
        return engine.infer(data)
    else:
        raise Exception(f'Unknown engine {config["engine"]}')

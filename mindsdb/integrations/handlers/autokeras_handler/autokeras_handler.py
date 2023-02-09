from typing import Optional

import dill
import pandas as pd

from mindsdb.integrations.libs.base import BaseMLEngine


class AutokerasHandler(BaseMLEngine):
    """
    Integration with the Ludwig declarative ML library.
    """  # noqa

    name = 'ludwig'

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[dict] = None) -> None:
        args = args['using']  # ignore the rest of the problem definition
        with open("hi.txt", "w+") as f:
            f.write("hi")

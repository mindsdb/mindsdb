from typing import Optional

import os
import dill
import pandas as pd
import autokeras as ak
from mindsdb.integrations.libs.base import BaseMLEngine

# Makes this run on Windows Subsystem for Linux
os.environ["XLA_FLAGS"]="--xla_gpu_cuda_data_dir=/usr/lib/cuda"

trainer_dict = {
    "regression": ak.StructuredDataRegressor
}

DEFAULT_MODE = "regression"
DEFAULT_EPOCHS = 1
DEFAULT_TRIALS = 1

def train_autokeras_model(training_df, target, mode=DEFAULT_MODE):
    trainer = trainer_dict[mode](overwrite=True, max_trials=DEFAULT_TRIALS)
    trainer.fit(training_df.drop(target, axis=1), training_df[target], epochs=DEFAULT_EPOCHS)
    return trainer.export_model()


class AutokerasHandler(BaseMLEngine):
    """
    Integration with the AutoKeras ML library.
    """  # noqa

    name = 'autokeras'

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[dict] = None) -> None:
        """
        Create and train model on the input df
        """
        args = args['using']  # ignore the rest of the problem definition

        
        training_df = df.drop(target, axis=1)
        model = train_autokeras_model(df, training_df)

        with open("hi.txt", "w+") as f:
            f.write("hi")

import pandas as pd
import unittest
from collections import OrderedDict

from mindsdb.integrations.handlers.sklearn_handler.sklearn_handler import SklearnHandler
from mindsdb.interfaces.storage.model_fs import ModelStorage
from mindsdb.interfaces.storage.json import JsonStorage
from mindsdb.interfaces.storage.fs import RESOURCE_GROUP


class TestSklearnHandler(unittest.TestCase):
    @property
    def dummy_training_data(self):
        return pd.DataFrame({
            'color': ['red', 'blue', 'green', 'red', 'blue'],
            'value': [5, 4, 3, 2, 1],
            'target': [0, 1, 0, 1, 0]
        })

    def setUp(self):
        self.model_storage = ModelStorage('sklearn_test_model')
        self.engine_storage = JsonStorage(resource_id='sklearn_test_engine', resource_group=RESOURCE_GROUP.INTEGRATION)
        self.handler = SklearnHandler(model_storage=self.model_storage, engine_storage=self.engine_storage)

    def test_create_and_predict(self):
        df = self.dummy_training_data.copy()
        self.handler.create(target='target', df=df, args={'target': 'target'})

        predictions = self.handler.predict(df.drop(columns=['target']))
        self.assertIn('target', predictions.columns)
        self.assertEqual(len(predictions), len(df))
        self.assertTrue(predictions['target'].notnull().all())


if __name__ == "__main__":
    unittest.main()

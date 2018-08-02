"""
*******************************************************
 * Copyright (C) 2017 MindsDB Inc. <copyright@mindsdb.com>
 *
 * This file is part of MindsDB Server.
 *
 * MindsDB Server can not be copied and/or distributed without the express
 * permission of MindsDB Inc
 *******************************************************
"""


import numpy
from config import *
from libs.constants.mindsdb import *
from libs.phases.base_module import BaseModule
from collections import OrderedDict
from libs.helpers.norm_denorm_helpers import denorm

class DataDevectorizer(BaseModule):

    phase_name = PHASE_DATA_DEVECTORIZATION

    def run(self):


        group_by = self.transaction.model_metadata[KEY_MODEL_GROUP_BY]
        group_by_index = None

        target_columns = [self.transaction.model_metadata[KEY_MODEL_PREDICT_COLUMNS]]

        # this is a template of how we store columns
        column_packs_template = OrderedDict()

        for i, column_name in enumerate(self.transaction.input_metadata[KEY_COLUMNS]):
            if group_by is not None and group_by == column_name:
                group_by_index = i

            column_packs_template[column_name] = []

        result = {}

        for test_train_all in self.transaction.model_data:
            result[test_train_all] = []
            for group in self.transaction.model_data[test_train_all]:
                for column in self.transaction.model_data[test_train_all][group]:
                    column_results = []
                    for value in self.transaction.model_data[test_train_all][group][column]:
                        stats = self.transaction.model_stats[column]
                        denormed = denorm(value=value,cell_stats=stats)
                        column_results.append(denormed)
                    result[test_train_all].append(column_results)
            result[test_train_all] = numpy.transpose(result[test_train_all])
            result[test_train_all] = result[test_train_all].tolist()
        return result



def test():
    from libs.test.test_controller import TestController
    module = TestController('CREATE MODEL FROM (SELECT * FROM Uploads.views.diamonds) AS diamonds PREDICT price', PHASE_DATA_DEVECTORIZATION)
    return

# only run the test if this file is called from debugger
if __name__ == "__main__":
    test()

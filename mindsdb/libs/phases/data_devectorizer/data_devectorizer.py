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
from mindsdb.config import *
from mindsdb.libs.constants.mindsdb import *
from mindsdb.libs.phases.base_module import BaseModule
from collections import OrderedDict
from mindsdb.libs.helpers.norm_denorm_helpers import denorm

class DataDevectorizer(BaseModule):

    phase_name = PHASE_DATA_DEVECTORIZATION

    def run(self):

        result = []

        #NOTE: we only use this model in PREDICT

        for group in self.transaction.model_data.predict_set:
            for column in self.transaction.model_data.predict_set[group]:
                column_results = []
                for value in self.transaction.model_data.predict_set[group][column]:
                    stats = self.transaction.model_stats[column]
                    denormed = denorm(value=value, cell_stats=stats)
                    column_results.append(denormed)
                result.append(column_results)

        # Why transponse?
        #result = numpy.transpose(result)
        #result = result.tolist()


        return result



def test():
    from mindsdb.libs.test.test_controller import TestController
    module = TestController('CREATE MODEL FROM (SELECT * FROM Uploads.views.diamonds) AS diamonds PREDICT price', PHASE_DATA_DEVECTORIZATION)
    return

# only run the test if this file is called from debugger
if __name__ == "__main__":
    test()

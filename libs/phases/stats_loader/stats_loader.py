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

import config as CONFIG
from libs.constants.mindsdb import *
from libs.phases.base_module import BaseModule

import sys
import json
import random
import traceback

class StatsLoader(BaseModule):

    phase_name = PHASE_DATA_STATS

    def run(self):

        model_name = self.transaction.predict_metadata[KEY_MODEL_NAME]
        stats = self.session.mongo.mindsdb.model_stats.find_one({KEY_MODEL_NAME: model_name})

        self.transaction.input_metadata = {
            KEY_COLUMNS: stats[KEY_COLUMNS]
        }

        self.transaction.model_stats = stats[KEY_STATS]
        self.transaction.model_metadata = stats[KEY_MODEL_METADATA]




def test():

    from libs.test.test_controller import TestController

    module = TestController('CREATE MODEL FROM (SELECT * FROM Uploads.views.tweets2 LIMIT 100) AS tweets4 PREDICT likes', PHASE_DATA_EXTRACTION)

    return

# only run the test if this file is called from debugger
if __name__ == "__main__":
    test()


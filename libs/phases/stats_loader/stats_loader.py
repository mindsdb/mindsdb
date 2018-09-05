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

        self.transaction.persistent_model_metadata = self.transaction.persistent_model_metadata.find_one(self.transaction.persistent_model_metadata.getPkey())





def test():

    from libs.test.test_controller import TestController

    module = TestController('CREATE MODEL FROM (SELECT * FROM Uploads.views.tweets2 LIMIT 100) AS tweets4 PREDICT likes', PHASE_DATA_EXTRACTION)

    return

# only run the test if this file is called from debugger
if __name__ == "__main__":
    test()


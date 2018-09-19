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

import json


def is_json(myjson):
    try:
        jsonObject = json.loads(myjson.decode('utf8'))
    except ValueError:
        return False
    return True


def get_json_data(data):
    if is_json(data):
        data = json.loads(data.decode('utf8'))
        return data
    return False

def json_to_string(data):
    try:
        return json.dumps(data).encode('utf8')
    except:
        return str(data).encode('utf8')
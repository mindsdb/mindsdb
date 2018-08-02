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

import re

def convert_cammelcase_to_snake_string(cammel_string):
    """
    Converts snake string to cammelcase

    :param cammel_string: as described
    :return: the snake string AsSaid -> as_said
    """

    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', cammel_string)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def get_label_index_for_value(value, labels):
    """
    This will define what index interval does a value belong to given a list of labels

    :param value: value to evaluate
    :param labels: the labels to compare against
    :return: an index value
    """
    if  value is None:
        return 0 # Todo: Add another index for None values

    if type(value) == type(''):
        try:
            index = labels.index(value)
        except:
            index = 0
    else:
        for index, compare_to in enumerate(labels):
            if value < compare_to:
                index = index
                break

    return index

def convert_snake_to_cammelcase_string(snake_str, first_lower = False):
    """
    Converts snake to cammelcase

    :param snake_str: as described
    :param first_lower: if you want to start the string with lowercase as_said -> asSaid
    :return: cammelcase string
    """

    components = snake_str.split('_')

    if first_lower == True:
        # We capitalize the first letter of each component except the first one
        # with the 'title' method and join them together.
        return components[0] + ''.join(x.title() for x in components[1:])
    else:
        return ''.join(x.title() for x in components)
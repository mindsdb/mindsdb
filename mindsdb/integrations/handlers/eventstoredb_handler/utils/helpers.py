from urllib.parse import urlunparse, urljoin
import re
import pandas as pd
import json
import base64

"""
From connection settings in MindsDB to an EventStoreDB AtomPub HTTP URL
#https://python.readthedocs.io/en/stable/library/urllib.parse.html#urllib.parse.urlunparse
"""


def build_basic_url(scheme, host, port):
    netloc = host + ":" + str(port)
    url = urlunparse([
        scheme,
        netloc,
        "", "", "", ""])
    return url


def build_health_url(basic_url):
    return urljoin(basic_url, "/health/live")


def build_streams_url(basic_url):
    return urljoin(basic_url, "streams/%24streams")


def build_stream_url(basic_url, stream_name):
    return urljoin(basic_url, "streams/" + stream_name)  # TODO: quote stream_name?


def build_stream_url_last_event(basic_url, stream_name):
    return urljoin(basic_url, "streams/" + stream_name + "/head/backward/1")


def build_next_url(link_url, read_batch_size):
    return re.sub(r"/(\d+)$", "/" + str(read_batch_size), link_url)


def entry_to_df(entry):
    # All events in EventStoreDB have the following:
    fields = ['eventId', 'eventType', 'eventNumber']
    df = pd.DataFrame([[entry['eventId'], entry['eventType'], entry['eventNumber']]],
                      columns=fields)
    data = pd.json_normalize(json.loads(entry['data']), sep='_')
    return df.merge(data, how='cross')


def get_auth_string(username, password):
    credentials = username + ':' + password
    return 'Basic ' + str(base64.b64encode(credentials.encode('utf-8')).decode('utf-8'))

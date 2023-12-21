import time
from datetime import datetime, timezone
from functools import wraps

import mindsdb.utilities.hooks as hooks
from mindsdb.utilities.config import Config
from mindsdb.utilities.context import context as ctx


def _get_current_node(profiling: dict) -> dict:
    """ return the node that the pointer points to

        Args:
            profiling (dict): whole profiling data

        Returns:
            dict: current node
    """
    current_node = profiling['tree']
    for child_index in profiling['pointer']:
        current_node = current_node['children'][child_index]
    return current_node


def start_node(tag: str):
    """ Add a new node to profiling

        Args:
            tag (str): name of new node
    """
    profiling = ctx.profiling
    new_node = {
        'start_at': time.perf_counter(),
        'start_at_thread': time.thread_time(),
        'start_at_process': time.process_time(),
        'stop_at': None,
        'name': tag,
        'children': []
    }
    if profiling['pointer'] is None:
        if profiling['level'] != 1:
            # profiling was activated not in the root of nodes tree
            return
        profiling['pointer'] = []
        profiling['tree'] = new_node
        profiling['tree']['time_start_at'] = datetime.now(timezone.utc)
    else:
        current_node = _get_current_node(profiling)
        profiling['pointer'].append(len(current_node['children']))
        current_node['children'].append(new_node)


def stop_current_node():
    """ Mark current node as completed and move pointer up
    """
    profiling = ctx.profiling
    if profiling['pointer'] is None:
        # profiling was activated not in the root of nodes tree
        return
    current_node = _get_current_node(profiling)
    current_node['stop_at'] = time.perf_counter()
    current_node['stop_at_thread'] = time.thread_time()
    current_node['stop_at_process'] = time.process_time()
    if len(profiling['pointer']) > 0:
        profiling['pointer'] = profiling['pointer'][:-1]
    else:
        if ctx.profiling['enabled'] is True:
            _send_profiling_results()
        profiling['pointer'] = None


def set_meta(**kwargs):
    """ Add any additional info to profiling data

        Args:
            **kwargs (dict): metadata to add
    """
    if profiling_enabled() is True:
        ctx.profiling.update(kwargs)


def _send_profiling_results():
    """ Send profiling results to storage
    """
    ctx.profiling['company_id'] = ctx.company_id
    ctx.profiling['hostname'] = Config().get('aws_meta_data', {}).get('public-hostname', '?')
    ctx.profiling['instance_id'] = Config().get('aws_meta_data', {}).get('instance-id', '?')
    hooks.send_profiling_results(ctx.profiling)


def enable():
    ctx.profiling['enabled'] = True


def disable():
    ctx.profiling['enabled'] = False


def profiling_enabled():
    try:
        return ctx.profiling['enabled'] is True
    except AttributeError:
        return False


def start(tag):
    """ add new node to profiling data
    """
    ctx.profiling['level'] += 1
    if profiling_enabled() is True:
        start_node(tag)


def stop():
    """ finalize current node and move pointer up
    """
    ctx.profiling['level'] -= 1
    if profiling_enabled() is True:
        stop_current_node()


class Context:
    def __init__(self, tag):
        self.tag = tag

    def __enter__(self):
        start(self.tag)

    def __exit__(self, exc_type, exc_value, traceback):
        stop()


def profile(tag: str = None):
    def decorator(function):
        @wraps(function)
        def wrapper(*args, **kwargs):
            if profiling_enabled() is True:
                with Context(tag or f'{function.__name__}|{function.__module__}'):
                    result = function(*args, **kwargs)
            else:
                result = function(*args, **kwargs)
            return result
        return wrapper
    return decorator

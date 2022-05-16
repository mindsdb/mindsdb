def empty_fn(*args, **kwargs):
    pass


try:
    from mindsdb.utilities.hooks.after_predict import after_predict
except ImportError:
    after_predict = empty_fn


try:
    from mindsdb.utilities.hooks.after_api_query import after_api_query
except ImportError:
    after_api_query = empty_fn

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
    after_palm_query = empty_fn


try:
    from mindsdb.utilities.hooks.profiling import send_profiling_results
except ImportError:
    send_profiling_results = empty_fn


try:
    from mindsdb.utilities.hooks.openai_query import before_openai_query
except ImportError:
    before_openai_query = empty_fn
    before_palm_query = empty_fn

try:
    from mindsdb.utilities.hooks.openai_query import after_openai_query
except ImportError:
    after_openai_query = empty_fn
    after_palm_query = empty_fn

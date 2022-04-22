def empty_fn(*args, **kwargs):
    pass


try:
    from mindsdb.utilities.hooks.after_predict import after_predict
except ImportError:
    after_predict = empty_fn

def empty_fn(*args, **kwargs):
    pass


try:
    from mindsdb.utilities.hooks.after_predict import after_predict
except ImportError:
    after_predict = empty_fn


try:
    from mindsdb.utilities.hooks.after_mysql_query import after_mysql_query
except ImportError:
    after_mysql_query = empty_fn


# class Hooks:
#     @property

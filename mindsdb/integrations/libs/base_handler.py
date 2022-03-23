class BaseHandler:
    def __init__(self, name):
        self.name = name

    def connect(self):
        pass

    def check_status(self):
        pass

    def get_tables(self):
        # show list of models
        pass

    def run_native_query(self, query_str):
        """ Inside this one, anything is valid because you assume no inter-operability """
        # create predictor
        # other custom syntax
        pass

    def select_query(self, statement):
        """ Here you can inter-operate betweens integrations. """
        pass

    def join(self, left_integration_instance, left_where, on=None):
        if not on:
            on = '*'
        pass

    def describe_table(self, table_name):
        """ For getting standard info about a table. e.g. data types """
        # @TODO: standard formatting
        pass


class DatabaseHandler(BaseHandler):
    def __init__(self, name):
        super().__init__(name)

    def get_views(self):
        pass

    def select_into(self, integration_instance, stmt):
        # not supported in predictor integrations @TODO: figure it out (interface)
        """ snf -> CTA.table_train """
        pass


class PredictiveHandler(BaseHandler):
    def __init__(self, name):
        super().__init__(name)





























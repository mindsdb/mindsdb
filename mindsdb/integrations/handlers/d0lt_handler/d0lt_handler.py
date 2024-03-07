from mindsdb.integrations.handlers.matrixone_handler.matrixone_handler import MatrixOneHandler


class D0ltHandler(MatrixOneHandler):
    """
    This handler handles connection and execution of the SQL  statements With D0lt.
    """

    name = "d0lt"

    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)

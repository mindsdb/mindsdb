from ..matrixone_handler import Handler as MatrixOneHandler


class D0ltHandler(MatrixOneHandler):
    """
    This handler handles connection and execution of the SQL  statements With D0lt.
    """
    name = 'd0lt'

    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)

import ray


class RayConnection:
    def __init__(self, addr=None, **kwargs):
        ray.init(address=addr, **kwargs)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        ray.shutdown()

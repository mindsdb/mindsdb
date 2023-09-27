try:
    import torch.multiprocessing as mp
except Exception:
    import multiprocessing as mp

ctx = mp.get_context('spawn')


class HandlerProcess(ctx.Process):
    daemon = True

    def __init__(self, fn, *args, **kwargs):
        super(HandlerProcess, self).__init__(args=args, kwargs=kwargs)
        self.fn = fn

    def run(self):
        self.fn(*self._args, **self._kwargs)

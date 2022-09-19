import torch.multiprocessing as mp

ctx = mp.get_context('spawn')


class HandlerProcess(ctx.Process):
    daemon = True

    def __init__(self, fn, *args):
        super(HandlerProcess, self).__init__(args=args)
        self.fn = fn

    def run(self):
        self.fn(*self._args)

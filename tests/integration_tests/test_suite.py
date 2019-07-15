from run_example import run_example
import multiprocessing


class NoDaemonProcess(multiprocessing.Process):
    @property
    def daemon(self):
        return False

    @daemon.setter
    def daemon(self, value):
        pass


class NoDaemonContext(type(multiprocessing.get_context())):
    Process = NoDaemonProcess

# We sub-class multiprocessing.pool.Pool instead of multiprocessing.Pool
# because the latter is only a wrapper function, not a proper class.
class DaemonPool(multiprocessing.Pool):
    def __init__(self, *args, **kwargs):
        kwargs['context'] = NoDaemonContext()
        super(DaemonPool, self).__init__(*args, **kwargs)

datasets = ['default_of_credit','home_rentals']

with DaemonPool(max(len(datasets),6)) as pool:
    pool.map(run_example,datasets)

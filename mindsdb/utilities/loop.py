import time
from torch.multiprocessing import Process


def periodic_executor(freq, func, args):
    while True:
        func(*args)
        
        time.sleep(freq)

def register(freq, func, args):
    print(f'Registered {func} with freq: {freq}')
    p = Process(target=periodic_executor, args=(freq, func, args))
    p.daemon = True
    p.start()
    #asyncio.get_running_loop().run_in_executor(None, periodic_executor, freq,func,args)

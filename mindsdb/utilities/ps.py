import sys
import time
import psutil


def net_connections():
    all_connections = []
    for p in psutil.process_iter(['pid']):
        try:
            process = psutil.Process(p.pid)
            connections = process.connections()
            if connections:
                all_connections += connections

        except (psutil.AccessDenied, psutil.ZombieProcess, psutil.NoSuchProcess):
            pass
    return all_connections


def is_port_in_use(port_num):
    portsinuse = []
    if sys.platform in ['darwin']:
        connection_func = net_connections
    else:
        connection_func = psutil.net_connections
    # conns = psutil.net_connections()
    conns = connection_func()
    portsinuse = [x.laddr[1] for x in conns if x.status == 'LISTEN']
    portsinuse.sort()
    return int(port_num) in portsinuse


def wait_func_is_true(func, timeout, *args, **kwargs):
    start_time = time.time()

    result = func(*args, **kwargs)
    while result is False and (time.time() - start_time) < timeout:
        time.sleep(2)
        result = func(*args, **kwargs)

    return result


def wait_port(port_num, timeout):
    return wait_func_is_true(func=is_port_in_use, timeout=timeout, port_num=port_num)


def get_listen_ports(pid):
    try:
        p = psutil.Process(pid)
        cons = p.connections()
        cons = [x.laddr.port for x in cons]
    except Exception:
        return []
    return cons


def is_pid_listen_port(pid, port):
    ports = get_listen_ports(pid)
    return int(port) in ports

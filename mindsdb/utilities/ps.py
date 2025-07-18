import sys
import time
from collections import namedtuple
import psutil


def get_child_pids(pid):
    p = psutil.Process(pid=pid)
    return p.children(recursive=True)


def net_connections():
    """Cross-platform psutil.net_connections like interface"""
    if sys.platform.lower().startswith("linux"):
        return psutil.net_connections()

    all_connections = []
    Pconn = None
    for p in psutil.process_iter(["pid"]):
        try:
            process = psutil.Process(p.pid)
            connections = process.net_connections()
            if connections:
                for conn in connections:
                    # Adding pid to the returned instance
                    # for consistency with psutil.net_connections()
                    if Pconn is None:
                        fields = list(conn._fields)
                        fields.append("pid")
                        _conn = namedtuple("Pconn", fields)
                    for attr in conn._fields:
                        setattr(_conn, attr, getattr(conn, attr))
                    _conn.pid = p.pid
                    all_connections.append(_conn)

        except (psutil.AccessDenied, psutil.ZombieProcess, psutil.NoSuchProcess):
            pass
    return all_connections


def is_port_in_use(port_num):
    """Check does any of child process uses specified port."""
    parent_process = psutil.Process()
    child_pids = [x.pid for x in parent_process.children(recursive=True)]
    conns = net_connections()
    portsinuse = [x.laddr[1] for x in conns if x.pid in child_pids and x.status == "LISTEN"]
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
        cons = p.net_connections()
        cons = [x.laddr.port for x in cons]
    except Exception:
        return []
    return cons


def is_pid_listen_port(pid, port):
    ports = get_listen_ports(pid)
    return int(port) in ports

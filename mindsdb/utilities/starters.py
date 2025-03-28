def start_http(*args, **kwargs):
    from mindsdb.api.http.start import start
    start(*args, **kwargs)


def start_mysql(*args, **kwargs):
    from mindsdb.api.mysql.start import start
    start(*args, **kwargs)


def start_mongo(*args, **kwargs):
    from mindsdb.api.mongo.start import start
    start(*args, **kwargs)


def start_postgres(*args, **kwargs):
    from mindsdb.api.postgres.start import start
    start(*args, **kwargs)


def start_tasks(*args, **kwargs):
    from mindsdb.interfaces.tasks.task_monitor import start
    start(*args, **kwargs)


def start_ml_task_queue(*args, **kwargs):
    from mindsdb.utilities.ml_task_queue.consumer import start
    start(*args, **kwargs)


def start_scheduler(*args, **kwargs):
    from mindsdb.interfaces.jobs.scheduler import start
    start(*args, **kwargs)


def start_mcp(verbose=False):
    """Start the MCP server"""
    from mindsdb.api.mcp.start import mcp
    from mindsdb.utilities.config import Config
    
    config = Config()
    port = config['api']['mcp'].get('port', 47335)
    host = config['api']['mcp'].get('host', '127.0.0.1')
    
    mcp.run(host=host, port=port)

def start_http(*args, **kwargs):
    from mindsdb.utilities.log import initialize_logging

    initialize_logging("http")

    from mindsdb.api.http.start import start

    start(*args, **kwargs)


def start_mysql(*args, **kwargs):
    from mindsdb.utilities.log import initialize_logging

    initialize_logging("mysql")

    from mindsdb.api.mysql.start import start

    start(*args, **kwargs)


def start_tasks(*args, **kwargs):
    from mindsdb.utilities.log import initialize_logging

    initialize_logging("tasks")

    from mindsdb.interfaces.tasks.task_monitor import start

    start(*args, **kwargs)


def start_ml_task_queue(*args, **kwargs):
    from mindsdb.utilities.log import initialize_logging

    initialize_logging("ml_task_queue")

    from mindsdb.utilities.ml_task_queue.consumer import start

    start(*args, **kwargs)


def start_scheduler(*args, **kwargs):
    from mindsdb.utilities.log import initialize_logging

    initialize_logging("scheduler")

    from mindsdb.interfaces.jobs.scheduler import start

    start(*args, **kwargs)


def start_litellm(*args, **kwargs):
    """Start the LiteLLM server"""
    from mindsdb.utilities.log import initialize_logging

    initialize_logging("litellm")

    from mindsdb.api.litellm.start import start

    start(*args, **kwargs)

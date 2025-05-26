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


def start_mcp(*args, **kwargs):
    """Start the MCP server"""
    from mindsdb.api.mcp.start import start

    start(*args, **kwargs)


def start_litellm(*args, **kwargs):
    """Start the LiteLLM server"""
    from mindsdb.api.litellm.start import start

    start(*args, **kwargs)


def start_a2a(*args, **kwargs):
    """Start the A2A server as a subprocess of the main MindsDB process"""
    from mindsdb.api.a2a.run_a2a import main

    # Extract configuration from the global config
    from mindsdb.utilities.config import Config
    config = Config()
    a2a_config = config.get('a2a', {})

    # Pass configuration to the A2A main function
    main(a2a_config, *args, **kwargs)

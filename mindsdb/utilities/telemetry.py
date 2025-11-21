"""Utilities for telemetry data collection and sending

Note: some libraries are imported in the functions, so they will not be imported if telemetry is disabled.
"""

import sys
from functools import wraps

from mindsdb.__about__ import __version__ as mindsdb_version
import mindsdb.interfaces.storage.db as db
from mindsdb.utilities.config import config
from mindsdb.utilities import log

logger = log.getLogger()


def if_telemetry_enabled(function):
    """Decorator to prevent calling the function if telemetry is disabled"""

    @wraps(function)
    def wrapper(*args, **kwargs):
        telemetry_enabled = config["telemetry"]["enabled"]
        if not telemetry_enabled or config.is_cloud:
            return
        return function(*args, **kwargs)

    return wrapper


def _get_all_telemetry_db_records():
    # fmt: off
    telemetry_records = (
        db.session.query(db.Telemetry)
        .filter_by(company_id=None)
        .order_by(db.Telemetry.created_at)
        .all()
    )
    return telemetry_records
    # fmt: on


@if_telemetry_enabled
def add_telemetry_record(event: str, record: dict) -> None:
    """Add a telemetry record to the database.

    Args:
        event: The event name.
        record: The record data.

    Returns:
        None
    """
    records_limit = config["telemetry"]["records_limit"]
    telemetry_records = _get_all_telemetry_db_records()
    try:
        if len(telemetry_records) + 1 > records_limit:
            for r in telemetry_records[: len(telemetry_records) + 1 - records_limit]:
                db.session.delete(r)
        record = db.Telemetry(
            company_id=None,
            event=event,
            record=record,
        )
        db.session.add(record)
        db.session.commit()
    except Exception as e:
        logger.error(f"Error adding telemetry record: {e}")
        db.session.rollback()


@if_telemetry_enabled
def create_telemetry_start_record() -> None:
    """Create a telemetry record for the start of the service."""
    import ast
    import platform
    import importlib
    from pathlib import Path

    try:
        # region collect datasources/integrations
        integration_records = db.session.query(db.Integration).filter_by(company_id=None).all()
        mindsdb_path = Path(importlib.util.find_spec("mindsdb").origin).parent
        handlers_path = mindsdb_path.joinpath("integrations/handlers")

        data_handlers = []
        for handler_dir in handlers_path.iterdir():
            if handler_dir.is_dir() is False or handler_dir.name.startswith("__"):
                continue
            init_file = handler_dir / "__init__.py"
            if not init_file.exists():
                continue
            code = ast.parse(init_file.read_text())

            is_data_handler = None
            handler_name = None
            for item in code.body:
                if isinstance(item, ast.Assign) and isinstance(item.targets[0], ast.Name):
                    name = item.targets[0].id
                    if isinstance(item.value, ast.Attribute) and name == "type":
                        is_data_handler = item.value.attr == "DATA"
                        if handler_name is not None:
                            break
                    if isinstance(item.value, ast.Constant) and name == "name":
                        handler_name = item.value.value
                        if is_data_handler is not None:
                            break
            if is_data_handler:
                data_handlers.append(handler_name)
        integration_engines = [record.engine for record in integration_records if record.engine in data_handlers]
        integrations_dict = {item: integration_engines.count(item) for item in set(integration_engines)}
        # endregion

        # region collect knowledge bases
        knowledge_base_records = db.session.query(db.KnowledgeBase).all()
        kb_result = []
        for kb in knowledge_base_records:
            params = kb.params or {}
            kb_result.append(
                {
                    "embedding_model": {
                        k: v for k, v in params.get("embedding_model", {}).items() if k in ("provider", "model_name")
                    },
                    "reranking_model": {
                        k: v for k, v in params.get("reranking_model", {}).items() if k in ("provider", "model_name")
                    },
                }
            )
        # endregion

        # region collect agents
        agent_records = db.session.query(db.Agents).filter_by(company_id=None).all()
        agents_list = []
        for agent in agent_records:
            params = agent.params or {}
            model_params = params.get("model", {})
            agents_list.append({k: v for k, v in model_params.items() if k in ("provider", "model_name")})
        # endregion
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error collecting telemetry data for 'start' event: {e}")

    record = {
        "mindsdb_version": mindsdb_version,
        "python_version": sys.version.split()[0],
        "os": {"system": platform.system()},
        "is_docker": config.use_docker_env,
        "datasources": integrations_dict,
        "knowledge_bases": kb_result,
        "agents": agents_list,
    }
    add_telemetry_record(event="start", record=record)


@if_telemetry_enabled
def send_telemetry() -> None:
    """Send the telemetry data to the telemetry service."""
    import requests
    import machineid

    machine_id = machineid.hashed_id("mindsdb")

    telemetry_records = _get_all_telemetry_db_records()

    if len(telemetry_records) == 0:
        return

    data = {"machine_id": machine_id, "records": []}
    for record in telemetry_records:
        data["records"].append({"event": record.event, **record.record})
    try:
        result = requests.post(config["telemetry"]["endpoint"], json=data, timeout=5)
        result.raise_for_status()
    except Exception:
        logger.warning("Telemetry data was not sent to the telemetry service")
    else:
        for record in telemetry_records:
            db.session.delete(record)
        db.session.commit()

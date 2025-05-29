#!/usr/bin/env python
"""Simple command-line tool to interact with an A2A server.

This script can:
  • fetch the agent card (metadata) from the server
  • send a single (non-streaming) task request
  • send a streaming request and live-print SSE events

Originally located at the repository root; restored to that location.
"""

from __future__ import annotations

import argparse
import json
import sys
import uuid
from typing import Iterator, Optional
import logging

import requests

DEFAULT_HOST = "localhost"
DEFAULT_PORT = 10002

###############################################################################
# Helper functions
###############################################################################


def _log_json(log_func, obj: dict, *, prefix: str = "") -> None:
    """Log JSON helper."""
    log_func(prefix + json.dumps(obj, indent=2))


def get_agent_info(
    a2a_host: str = DEFAULT_HOST,
    a2a_port: int = DEFAULT_PORT,
) -> Optional[dict]:
    """Retrieve the agent card from `/.well-known/agent.json` (or legacy path)."""

    url = f"http://{a2a_host}:{a2a_port}/.well-known/agent.json"

    logging.debug(f"Fetching agent info from {url} …")

    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 404:
            legacy_url = f"http://{a2a_host}:{a2a_port}/agent-card"
            logging.debug(f"Not found, trying legacy path {legacy_url}")
            response = requests.get(legacy_url, timeout=10)

        if response.ok:
            card = response.json()
            _log_json(logging.debug, card, prefix="Received agent card:\n")
            return card

        logging.debug(f"Failed to fetch agent card – status: {response.status_code}")
    except requests.RequestException as exc:
        logging.debug(f"Connection error while fetching agent card: {exc}")

    return None


###############################################################################
# Task helpers
###############################################################################


def _new_ids() -> tuple[str, str, str]:
    """Generate fresh UUID4 strings for taskId, sessionId, requestId."""

    return uuid.uuid4().hex, uuid.uuid4().hex, uuid.uuid4().hex


def _post_json(url: str, payload: dict, *, stream: bool = False) -> requests.Response:
    headers = {"Content-Type": "application/json"}
    if stream:
        headers["Accept"] = "text/event-stream"
    return requests.post(
        url, json=payload, headers=headers, stream=stream, timeout=None
    )


###############################################################################
# Non-streaming request
###############################################################################


def send_a2a_query(
    query: str,
    *,
    a2a_host: str = DEFAULT_HOST,
    a2a_port: int = DEFAULT_PORT,
) -> bool:
    """Send a *blocking* `tasks/send` request and print the result."""

    task_id, session_id, request_id = _new_ids()

    payload = {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": "tasks/send",
        "params": {
            "id": task_id,
            "sessionId": session_id,
            "message": {
                "role": "user",
                "parts": [
                    {"type": "text", "text": query},
                ],
            },
            "acceptedOutputModes": ["text/plain"],
        },
    }

    url = f"http://{a2a_host}:{a2a_port}/a2a"
    logging.debug(f"POST {url}")
    _log_json(logging.debug, payload, prefix="Request →\n")
    logging.info("Sending query …")

    try:
        response = _post_json(url, payload)
    except requests.RequestException as exc:
        logging.error(f"⚠️  Network error: {exc}")
        return False

    if not response.ok:
        logging.error(f"⚠️  HTTP {response.status_code}\n{response.text}")
        return False

    try:
        data = response.json()
    except ValueError:
        logging.error(f"⚠️  Invalid JSON response: {response.text[:200]}")
        return False

    _log_json(logging.debug, data, prefix="Full response ←\n")

    if "error" in data:
        logging.error(f"⚠️  RPC error: {data['error'].get('message')}")
        return False

    result = data.get("result")
    if not result:
        logging.error("⚠️  No result field in response")
        return False

    # Print status/"thinking" message
    status = result.get("status", {})
    msg_parts = status.get("message", {}).get("parts", [])
    if msg_parts:
        logging.info("\nAgent thinking:")
        _log_parts(logging.info, msg_parts)

    # Print artifacts (agent answer)
    artifacts = result.get("artifacts") or []
    if artifacts:
        logging.info("\nAgent response:")
        for artifact in artifacts:
            _log_parts(logging.info, artifact.get("parts", []))
    else:
        logging.info("(No artifacts returned)")

    return True


###############################################################################
# Streaming request helpers
###############################################################################


def _iter_sse_lines(resp: requests.Response) -> Iterator[str]:
    """Yield raw Server-Sent-Event lines (decoded)."""
    buf = ""
    for chunk in resp.iter_content(chunk_size=1024):
        if not chunk:
            continue
        buf += chunk.decode()
        while "\n" in buf:
            line, buf = buf.split("\n", 1)
            yield line.rstrip("\r")


def _parse_sse_event(lines: list[str]) -> dict | None:
    """Parse an SSE event block into a dict."""
    data_lines = [line_content[6:] for line_content in lines if line_content.startswith("data: ")]
    if not data_lines:
        return None
    try:
        return json.loads("\n".join(data_lines))
    except json.JSONDecodeError:
        return None


def send_streaming_query(
    query: str,
    *,
    a2a_host: str = DEFAULT_HOST,
    a2a_port: int = DEFAULT_PORT,
) -> bool:
    """Send a `tasks/sendSubscribe` request and stream responses as they arrive."""

    task_id, session_id, request_id = _new_ids()

    payload = {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": "tasks/sendSubscribe",
        "params": {
            "id": task_id,
            "sessionId": session_id,
            "message": {
                "role": "user",
                "parts": [
                    {"type": "text", "text": query},
                ],
            },
            "acceptedOutputModes": ["text/plain"],
        },
    }

    url = f"http://{a2a_host}:{a2a_port}/a2a"
    logging.debug(f"POST (stream) {url}")
    _log_json(logging.debug, payload, prefix="Request →\n")

    try:
        response = _post_json(url, payload, stream=True)
    except requests.RequestException as exc:
        logging.error(f"⚠️  Network error: {exc}")
        return False

    if not response.ok:
        logging.error(f"⚠️  HTTP {response.status_code}\n{response.text}")
        return False

    logging.info("Streaming events (Ctrl-C to abort):")

    try:
        event_lines: list[str] = []
        for line in _iter_sse_lines(response):
            if line == "":  # blank = dispatch event
                event = _parse_sse_event(event_lines)
                event_lines.clear()
                if event is not None:
                    _handle_stream_event(event)
                continue
            event_lines.append(line)
    except KeyboardInterrupt:
        logging.info("\nInterrupted by user.")
        return False

    return True


###############################################################################
# Output helpers
###############################################################################


def _log_parts(log_func, parts: list[dict]) -> None:
    for part in parts:
        if part.get("type") == "text":
            log_func(part.get("text", ""))
        elif part.get("type") == "data":
            log_func("\nStructured data:")
            _log_json(log_func, part.get("data", {}))


def _handle_stream_event(event: dict) -> None:
    """Handle a single SSE event parsed as JSON.

    The server sends JSON-RPC messages of the form::

        {"jsonrpc":"2.0","id":"…","result":{…}}

    where *result* is either a TaskStatusUpdateEvent or a TaskArtifactUpdateEvent.
    These objects do not carry an explicit ``type`` field, so we infer it based
    on their keys.
    """

    # If logging level is DEBUG, log the raw event and mimic the original 'return' behavior
    if logging.getLogger().isEnabledFor(logging.DEBUG):
        _log_json(logging.debug, event, prefix="Raw Event (DEBUG) ← ")
        # This return mimics the original behavior where verbose mode would
        # print the raw event and skip further processing for this event.
        return

    # If the server uses an older "typed" envelope just keep legacy handling
    if "type" in event:
        etype = event["type"]
        if etype == "status":
            message = event.get("status", {}).get("message", {})
            parts = message.get("parts", [])
            if parts:
                _log_parts(logging.info, parts)
        elif etype == "artifact":
            artifacts = event.get("artifacts") or []
            for artifact in artifacts:
                _log_parts(logging.info, artifact.get("parts", []))
        elif etype == "end":
            logging.info("\n[stream end]")
        return

    # ---- New A2A 0.2 style messages ---------------------------------------
    # Extract "result" or "error" from the JSON-RPC envelope.
    if "error" in event:
        err = event["error"]
        logging.error(f"RPC error: {err.get('message')}")
        return

    result = event.get("result")
    if not result:
        # Nothing useful to display
        return

    # Status update?  (TaskStatusUpdateEvent)
    if "status" in result:
        message = result["status"].get("message", {})
        parts = message.get("parts", [])
        if parts:
            _log_parts(logging.info, parts)

        # If final flag present we can acknowledge.
        if result.get("final"):
            logging.info("\n[completed]")
        return

    # Artifact update? (TaskArtifactUpdateEvent)
    if "artifact" in result:
        artifact = result["artifact"]
        _log_parts(logging.info, artifact.get("parts", []))
        return


###############################################################################
# CLI argument parsing
###############################################################################


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Interact with an A2A server")

    parser.add_argument(
        "query", nargs="*", help="Query to send. If omitted, just fetch agent card."
    )

    parser.add_argument(
        "--host", default=DEFAULT_HOST, help="A2A host (default: %(default)s)"
    )
    parser.add_argument(
        "--port", type=int, default=DEFAULT_PORT, help="A2A port (default: %(default)s)"
    )

    parser.add_argument(
        "--stream",
        action="store_true",
        help="Use streaming subscribe request instead of blocking send",
    )

    parser.add_argument("--verbose", action="store_true", help="Verbose output (debug)")

    return parser.parse_args(argv)


###############################################################################
# Main entry-point
###############################################################################


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    # Configure logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(format="%(message)s", level=log_level, stream=sys.stdout)
    if args.verbose:
        logging.debug("Verbose mode enabled.")

    # 1. Always fetch agent-card first (helps verify server is live)
    agent_info = get_agent_info(args.host, args.port)
    if agent_info is None:
        logging.critical("⚠️  Could not retrieve agent info – abort")
        sys.exit(1)

    if not args.query:
        # Enter interactive REPL style mode
        logging.info("Interactive mode – type 'exit' or Ctrl-D to quit.")
        try:
            while True:
                try:
                    user_input = input("> ").strip()
                except EOFError:
                    # Ctrl-D
                    logging.info("")
                    break

                if user_input.lower() in {"exit", "quit"}:
                    break

                if not user_input:
                    continue

                ok = (
                    send_streaming_query(
                        user_input,
                        a2a_host=args.host,
                        a2a_port=args.port,
                    )
                    if args.stream
                    else send_a2a_query(
                        user_input,
                        a2a_host=args.host,
                        a2a_port=args.port,
                    )
                )

                # Add separator between queries
                if ok:
                    logging.info("\n" + "—" * 30)
        except KeyboardInterrupt:
            # Ctrl-C to exit
            logging.info("")
        sys.exit(0)

    # Single query passed via CLI
    query_str = " ".join(args.query)

    ok = (
        send_streaming_query(query_str, a2a_host=args.host, a2a_port=args.port)
        if args.stream
        else send_a2a_query(query_str, a2a_host=args.host, a2a_port=args.port)
    )

    sys.exit(0 if ok else 2)


if __name__ == "__main__":
    main()

"""MCP server exposing the scheduler as tools for AI agents.

Run with:
    python -m apscheduler.agents.mcp_server [--scheduler-url http://localhost:8080]

This connects to a running apscheduler-daemon HTTP API and exposes its
endpoints as MCP tools. AI agents (Claude Desktop, Claude Code, Cursor, etc)
can then list, pause, trigger, and inspect jobs through natural language.
"""
from __future__ import annotations

import json
import logging
import os
import sys
from typing import Any

logger = logging.getLogger(__name__)


def _http_get(url: str, path: str) -> dict:
    import urllib.request
    req = urllib.request.Request(f"{url}{path}")
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode())


def _http_post(url: str, path: str, data: "dict | None" = None) -> dict:
    import urllib.request
    body = json.dumps(data or {}).encode()
    req = urllib.request.Request(
        f"{url}{path}",
        data=body,
        headers={'Content-Type': 'application/json'},
        method='POST',
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode())


def list_tools() -> "list[dict]":
    """Return the MCP tool definitions."""
    return [
        {
            "name": "list_jobs",
            "description": "List all scheduled jobs in the scheduler.",
            "inputSchema": {"type": "object", "properties": {}},
        },
        {
            "name": "get_job",
            "description": "Get details about a specific job by ID.",
            "inputSchema": {
                "type": "object",
                "properties": {"job_id": {"type": "string"}},
                "required": ["job_id"],
            },
        },
        {
            "name": "pause_job",
            "description": "Pause a job so it stops firing on schedule.",
            "inputSchema": {
                "type": "object",
                "properties": {"job_id": {"type": "string"}},
                "required": ["job_id"],
            },
        },
        {
            "name": "resume_job",
            "description": "Resume a paused job.",
            "inputSchema": {
                "type": "object",
                "properties": {"job_id": {"type": "string"}},
                "required": ["job_id"],
            },
        },
        {
            "name": "trigger_job",
            "description": "Trigger a job to run immediately, regardless of its schedule.",
            "inputSchema": {
                "type": "object",
                "properties": {"job_id": {"type": "string"}},
                "required": ["job_id"],
            },
        },
        {
            "name": "remove_job",
            "description": "Permanently remove a job from the scheduler.",
            "inputSchema": {
                "type": "object",
                "properties": {"job_id": {"type": "string"}},
                "required": ["job_id"],
            },
        },
        {
            "name": "get_job_history",
            "description": "Get recent execution history for a job.",
            "inputSchema": {
                "type": "object",
                "properties": {"job_id": {"type": "string"}},
                "required": ["job_id"],
            },
        },
        {
            "name": "get_next_fire_times",
            "description": "Get the next N fire times for a job.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "job_id": {"type": "string"},
                    "n": {"type": "integer", "default": 5},
                },
                "required": ["job_id"],
            },
        },
        {
            "name": "scheduler_status",
            "description": "Get overall scheduler status (running, job count, uptime).",
            "inputSchema": {"type": "object", "properties": {}},
        },
        {
            "name": "pause_scheduler",
            "description": "Pause the entire scheduler.",
            "inputSchema": {"type": "object", "properties": {}},
        },
        {
            "name": "resume_scheduler",
            "description": "Resume the entire scheduler.",
            "inputSchema": {"type": "object", "properties": {}},
        },
        {
            "name": "list_budgets",
            "description": (
                "List all in-process cost budgets and their current state. "
                "Note: this only returns budgets registered in the same process "
                "as the MCP server. If the scheduler runs in a separate daemon, "
                "use the daemon HTTP API instead."
            ),
            "inputSchema": {"type": "object", "properties": {}},
        },
        {
            "name": "get_budget",
            "description": (
                "Get the status of a single cost budget by name. "
                "Same in-process limitation as list_budgets."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {"name": {"type": "string"}},
                "required": ["name"],
            },
        },
        {
            "name": "pause_jobs_in_budget",
            "description": (
                "Pause all jobs that share the given budget. "
                "Requires budget->job tracking; currently a stub that returns "
                "an error indicating the wiring is incomplete."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {"name": {"type": "string"}},
                "required": ["name"],
            },
        },
    ]


def call_tool(name: str, args: dict, scheduler_url: str) -> Any:
    """Execute a tool call against the scheduler API."""
    if name == "list_jobs":
        return _http_get(scheduler_url, "/api/v1/jobs")
    if name == "get_job":
        return _http_get(scheduler_url, f"/api/v1/jobs/{args['job_id']}")
    if name == "pause_job":
        return _http_post(scheduler_url, f"/api/v1/jobs/{args['job_id']}/pause")
    if name == "resume_job":
        return _http_post(scheduler_url, f"/api/v1/jobs/{args['job_id']}/resume")
    if name == "trigger_job":
        return _http_post(scheduler_url, f"/api/v1/jobs/{args['job_id']}/run")
    if name == "remove_job":
        import urllib.request
        req = urllib.request.Request(
            f"{scheduler_url}/api/v1/jobs/{args['job_id']}", method='DELETE'
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            return {"removed": True}
    if name == "get_job_history":
        return _http_get(scheduler_url, f"/api/v1/jobs/{args['job_id']}/history")
    if name == "get_next_fire_times":
        n = args.get('n', 5)
        return _http_get(scheduler_url, f"/api/v1/jobs/{args['job_id']}/next?n={n}")
    if name == "scheduler_status":
        return _http_get(scheduler_url, "/api/v1/scheduler")
    if name == "pause_scheduler":
        return _http_post(scheduler_url, "/api/v1/scheduler/pause")
    if name == "resume_scheduler":
        return _http_post(scheduler_url, "/api/v1/scheduler/resume")
    if name == "list_budgets":
        from apscheduler.agents.budget import _BUDGETS, _REGISTRY_LOCK
        with _REGISTRY_LOCK:
            return {bname: b.status() for bname, b in _BUDGETS.items()}
    if name == "get_budget":
        from apscheduler.agents.budget import _BUDGETS, _REGISTRY_LOCK
        bname = args['name']
        with _REGISTRY_LOCK:
            b = _BUDGETS.get(bname)
            if b is None:
                raise ValueError(f"Budget {bname!r} not found")
            return b.status()
    if name == "pause_jobs_in_budget":
        return {
            "error": "Not implemented: budget->job tracking is not wired yet",
            "budget": args.get('name'),
        }
    raise ValueError(f"Unknown tool: {name}")


def run_stdio_server(scheduler_url: str) -> None:
    """Run an MCP stdio server (JSON-RPC over stdin/stdout)."""
    logger.info(f"MCP server connecting to scheduler at {scheduler_url}")

    while True:
        line = sys.stdin.readline()
        if not line:
            break
        try:
            req = json.loads(line)
        except json.JSONDecodeError:
            continue

        method = req.get('method')
        params = req.get('params', {})
        req_id = req.get('id')

        try:
            if method == 'initialize':
                result = {
                    'protocolVersion': '2024-11-05',
                    'capabilities': {'tools': {}},
                    'serverInfo': {'name': 'apscheduler-rs', 'version': '0.1.0'},
                }
            elif method == 'tools/list':
                result = {'tools': list_tools()}
            elif method == 'tools/call':
                tool_name = params.get('name')
                tool_args = params.get('arguments', {})
                output = call_tool(tool_name, tool_args, scheduler_url)
                result = {
                    'content': [
                        {'type': 'text', 'text': json.dumps(output, indent=2, default=str)}
                    ]
                }
            else:
                raise ValueError(f"Unknown method: {method}")

            response = {'jsonrpc': '2.0', 'id': req_id, 'result': result}
        except Exception as e:
            response = {
                'jsonrpc': '2.0',
                'id': req_id,
                'error': {'code': -32000, 'message': str(e)},
            }

        sys.stdout.write(json.dumps(response) + '\n')
        sys.stdout.flush()


def main():
    import argparse
    parser = argparse.ArgumentParser(description='APScheduler-rs MCP server')
    parser.add_argument(
        '--scheduler-url',
        default=os.environ.get('APSCHEDULER_URL', 'http://localhost:8080'),
        help='URL of the apscheduler-daemon HTTP API',
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s', stream=sys.stderr)
    run_stdio_server(args.scheduler_url)


if __name__ == '__main__':
    main()

"""Webhook-triggered jobs - fire jobs in response to HTTP events instead of time."""
from __future__ import annotations

import logging
import threading
from typing import Any, Optional
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# Registry: webhook_path -> list of (scheduler, job_id, secret) tuples
_WEBHOOK_LISTENERS: "dict[str, list]" = {}
_REGISTRY_LOCK = threading.Lock()


class WebhookTrigger:
    """A trigger that fires when a webhook is received.

    Use scheduler.add_webhook_job() instead of constructing this directly.
    """
    def __init__(self, path: str, secret: Optional[str] = None):
        self.path = path
        self.secret = secret

    def __repr__(self):
        return f"WebhookTrigger(path={self.path!r})"


def register_webhook_listener(path: str, scheduler, job_id: str, secret: Optional[str] = None) -> None:
    """Register a webhook path -> job binding."""
    with _REGISTRY_LOCK:
        if path not in _WEBHOOK_LISTENERS:
            _WEBHOOK_LISTENERS[path] = []
        _WEBHOOK_LISTENERS[path].append((scheduler, job_id, secret))
        logger.info(f"Registered webhook listener: {path} -> {job_id}")


def fire_webhook(path: str, payload: Any = None, headers: Optional[dict] = None) -> int:
    """Fire all jobs bound to a webhook path. Returns the number triggered."""
    with _REGISTRY_LOCK:
        listeners = _WEBHOOK_LISTENERS.get(path, [])[:]

    fired = 0
    for scheduler, job_id, secret in listeners:
        try:
            # Verify secret if set (simple shared-secret check)
            if secret and headers and headers.get('X-Webhook-Secret') != secret:
                continue
            # Trigger the job by setting next_run_time to now
            scheduler.modify_job(job_id, next_run_time=datetime.now(timezone.utc))
            fired += 1
        except Exception as e:
            logger.warning(f"Failed to fire webhook job {job_id}: {e}")
    return fired


def serve_webhooks(host: str = '127.0.0.1', port: int = 8765) -> None:
    """Run a tiny HTTP server that fires webhook listeners.

    Routes:
        POST /hook/<path> - fire all listeners on that path

    Use a separate process/thread for production. This is a convenience wrapper.
    """
    import http.server
    import json as _json

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_POST(self):
            if not self.path.startswith('/hook/'):
                self.send_response(404)
                self.end_headers()
                return
            hook_path = self.path[len('/hook/'):]
            length = int(self.headers.get('Content-Length', '0') or '0')
            body = self.rfile.read(length) if length else b''
            try:
                payload = _json.loads(body) if body else None
            except Exception:
                payload = body.decode('utf-8', errors='replace')
            headers = dict(self.headers)
            count = fire_webhook(hook_path, payload, headers)
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(_json.dumps({'fired': count}).encode())

        def log_message(self, format, *args):
            pass  # silent

    server = http.server.HTTPServer((host, port), Handler)
    logger.info(f"Webhook server listening on http://{host}:{port}")
    server.serve_forever()

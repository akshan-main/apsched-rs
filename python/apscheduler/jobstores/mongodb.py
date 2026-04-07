"""MongoDB job store compatibility layer.

This module provides an APScheduler 3.x-compatible MongoDB job store
interface. The actual persistence is handled by the native Rust MongoDB
store engine for maximum performance.
"""

from __future__ import annotations

try:
    from apscheduler._rust import MongoJobStore as _RustMongoJobStore
except ImportError:  # pragma: no cover - only when the extension is unavailable
    _RustMongoJobStore = None


class MongoDBJobStore:
    """APScheduler 3.x-compatible MongoDB job store.

    Drop-in replacement for ``apscheduler.jobstores.mongodb.MongoDBJobStore``.
    The actual MongoDB operations are performed by the Rust-native store so
    users get the same API with substantially better throughput.

    Args:
        database: Database name (default ``"apscheduler"``).
        collection: Collection name (default ``"jobs"``).
        client: An existing ``pymongo`` client instance. If provided, its
            address is used to construct a connection URI for the Rust
            backend. Any additional client options supplied to pymongo are
            not forwarded automatically -- pass them via ``uri`` if needed.
        uri: Explicit MongoDB connection URI (e.g.
            ``"mongodb://localhost:27017"``). Takes precedence over
            ``host``/``port``.
        host: Hostname, used together with ``port`` when ``uri`` is not
            given (default ``"localhost"``).
        port: TCP port, used together with ``host`` when ``uri`` is not
            given (default ``27017``).
        **kwargs: Ignored extra options for forward/backward compatibility
            with APScheduler 3.x call sites.
    """

    def __init__(
        self,
        database: str = "apscheduler",
        collection: str = "jobs",
        client=None,
        uri: str | None = None,
        host: str = "localhost",
        port: int = 27017,
        **kwargs,
    ) -> None:
        if uri is None:
            if client is not None:
                # Extract an address from a pymongo client if possible.
                resolved = None
                try:
                    address = getattr(client, "address", None)
                    if address is not None:
                        resolved = f"mongodb://{address[0]}:{address[1]}"
                except Exception:
                    resolved = None
                if resolved is None:
                    try:
                        nodes = getattr(client, "nodes", None)
                        if nodes:
                            node = next(iter(nodes))
                            resolved = f"mongodb://{node[0]}:{node[1]}"
                    except Exception:
                        resolved = None
                uri = resolved or f"mongodb://{host}:{port}"
            else:
                uri = f"mongodb://{host}:{port}"

        self.uri = uri
        self.database = database
        self.collection = collection
        self._client = client
        self._rust_store = None

        if _RustMongoJobStore is not None:
            self._rust_store = _RustMongoJobStore(
                uri=uri,
                database=database,
                collection=collection,
            )

    @property
    def inner(self):
        """Access the underlying Rust store, if available."""
        return self._rust_store

    def __repr__(self) -> str:
        return (
            f"MongoDBJobStore(uri={self.uri!r}, database={self.database!r}, "
            f"collection={self.collection!r})"
        )


__all__ = ["MongoDBJobStore"]

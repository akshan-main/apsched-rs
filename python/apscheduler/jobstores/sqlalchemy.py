"""SQLAlchemy job store compatibility layer.

This module provides an APScheduler 3.x-compatible SQLAlchemy job store interface.
The actual persistence is handled by the Rust SQL store engine.
"""

try:
    from apscheduler._rust import SqlJobStore as _RustSqlJobStore
except ImportError:
    _RustSqlJobStore = None


class SQLAlchemyJobStore:
    """APScheduler 3.x-compatible SQLAlchemy job store.

    This is a compatibility wrapper. The actual SQL operations are performed
    by the Rust SQL store engine for maximum performance.

    Args:
        url: Database URL (e.g., 'sqlite:///jobs.db', 'postgresql://...')
        tablename: Name of the table to store jobs in (default: 'apscheduler_jobs')
        engine: SQLAlchemy engine instance (alternative to url)
        tableschema: Schema name for the table
        pickle_protocol: Pickle protocol version for serialization
    """

    def __init__(
        self,
        url: str | None = None,
        engine: object | None = None,
        tablename: str = "apscheduler_jobs",
        tableschema: str | None = None,
        pickle_protocol: int = 2,
    ) -> None:
        self.url = url
        self.tablename = tablename
        self.tableschema = tableschema
        self.pickle_protocol = pickle_protocol
        self._engine = engine
        self._rust_store = None

        # If the Rust backend is available and we have a URL, initialize it
        if _RustSqlJobStore is not None and url is not None:
            self._rust_store = _RustSqlJobStore(url=url, tablename=tablename)

    @property
    def inner(self):
        """Access the underlying Rust store, if available."""
        if self._rust_store is not None:
            return self._rust_store
        return None

    def __repr__(self) -> str:
        return f"SQLAlchemyJobStore(url={self.url!r}, tablename={self.tablename!r})"


__all__ = ["SQLAlchemyJobStore"]

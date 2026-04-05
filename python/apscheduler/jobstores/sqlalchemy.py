"""SQLAlchemy job store compatibility layer.

This module provides an APScheduler 3.x-compatible SQLAlchemy job store interface.
The actual persistence is handled by the Rust SQL store engine.
"""


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
        # Engine will be initialized when the scheduler starts
        self._engine = engine

    def __repr__(self) -> str:
        return f"SQLAlchemyJobStore(url={self.url!r}, tablename={self.tablename!r})"


__all__ = ["SQLAlchemyJobStore"]

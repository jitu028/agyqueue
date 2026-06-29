import sqlite3
import os
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Optional, List
from contextlib import contextmanager
from agyqueue.models import Task, TaskStatus

logger = logging.getLogger(__name__)

class BaseTaskStore(ABC):
    """Abstract base class defining the interface for task storage backends."""
    
    @abstractmethod
    def save_task(self, task: Task) -> None:
        """Saves a new task or updates an existing one."""
        pass

    @abstractmethod
    def get_task(self, task_id: str) -> Optional[Task]:
        """Retrieves a task by its unique ID."""
        pass

    @abstractmethod
    def update_task(
        self,
        task_id: str,
        status: TaskStatus,
        progress: int,
        step: str,
        result: Optional[str] = None,
        error: Optional[str] = None,
    ) -> Optional[Task]:
        """Updates the status, progress, current step, and optionally result/error of a task."""
        pass

    @abstractmethod
    def touch_task(self, task_id: str) -> None:
        """Touches the task to update its updated_at heartbeat timestamp."""
        pass

    @abstractmethod
    def list_tasks(self, namespace: Optional[str] = None) -> List[Task]:
        """Lists tasks ordered by creation date desc, optionally filtered by namespace."""
        pass

    @abstractmethod
    def get_subtasks(self, parent_id: str) -> List[Task]:
        """Lists all child subtasks for a given parent task."""
        pass


class SQLiteTaskStore(BaseTaskStore):
    """SQLite implementation of the TaskStore interface."""
    
    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or os.environ.get("AGYQUEUE_DB_PATH", "agyqueue.db")
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        # Enable WAL mode for better concurrency in multi-process environments
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        with self._get_conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS tasks (
                    task_id TEXT PRIMARY KEY,
                    prompt TEXT NOT NULL,
                    task_type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    progress INTEGER NOT NULL,
                    step TEXT NOT NULL,
                    result TEXT,
                    error TEXT,
                    parent_id TEXT,
                    namespace TEXT NOT NULL DEFAULT 'default',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
            """)
            conn.commit()
            
            # Schema migration: add parent_id if table already existed without it
            try:
                conn.execute("ALTER TABLE tasks ADD COLUMN parent_id TEXT;")
                conn.commit()
            except sqlite3.OperationalError:
                pass

            # Schema migration: add namespace if table already existed without it
            try:
                conn.execute("ALTER TABLE tasks ADD COLUMN namespace TEXT NOT NULL DEFAULT 'default';")
                conn.commit()
            except sqlite3.OperationalError:
                pass

    def save_task(self, task: Task) -> None:
        with self._get_conn() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO tasks 
                (task_id, prompt, task_type, status, progress, step, result, error, parent_id, namespace, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    task.task_id,
                    task.prompt,
                    task.task_type,
                    task.status.value,
                    task.progress,
                    task.step,
                    task.result,
                    task.error,
                    task.parent_id,
                    task.namespace,
                    task.created_at,
                    task.updated_at,
                ),
            )
            conn.commit()

    def get_task(self, task_id: str) -> Optional[Task]:
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT * FROM tasks WHERE task_id = ?", (task_id,)
            ).fetchone()
            if not row:
                return None
            return Task(
                task_id=row["task_id"],
                prompt=row["prompt"],
                task_type=row["task_type"],
                status=TaskStatus(row["status"]),
                progress=row["progress"],
                step=row["step"],
                result=row["result"],
                error=row["error"],
                parent_id=row["parent_id"],
                namespace=row["namespace"] if "namespace" in row.keys() else "default",
                created_at=row["created_at"],
                updated_at=row["updated_at"],
            )

    def update_task(
        self,
        task_id: str,
        status: TaskStatus,
        progress: int,
        step: str,
        result: Optional[str] = None,
        error: Optional[str] = None,
    ) -> Optional[Task]:
        now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
        with self._get_conn() as conn:
            conn.execute(
                """
                UPDATE tasks
                SET status = ?, progress = ?, step = ?, result = COALESCE(?, result), error = COALESCE(?, error), updated_at = ?
                WHERE task_id = ?
                """,
                (status.value, progress, step, result, error, now, task_id),
            )
            conn.commit()
        return self.get_task(task_id)

    def touch_task(self, task_id: str) -> None:
        now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
        with self._get_conn() as conn:
            conn.execute(
                "UPDATE tasks SET updated_at = ? WHERE task_id = ?",
                (now, task_id),
            )
            conn.commit()

    def list_tasks(self, namespace: Optional[str] = None) -> List[Task]:
        with self._get_conn() as conn:
            if namespace:
                rows = conn.execute("SELECT * FROM tasks WHERE namespace = ? ORDER BY created_at DESC", (namespace,)).fetchall()
            else:
                rows = conn.execute("SELECT * FROM tasks ORDER BY created_at DESC").fetchall()
            return [
                Task(
                    task_id=row["task_id"],
                    prompt=row["prompt"],
                    task_type=row["task_type"],
                    status=TaskStatus(row["status"]),
                    progress=row["progress"],
                    step=row["step"],
                    result=row["result"],
                    error=row["error"],
                    parent_id=row["parent_id"],
                    namespace=row["namespace"] if "namespace" in row.keys() else "default",
                    created_at=row["created_at"],
                    updated_at=row["updated_at"],
                )
                for row in rows
            ]

    def get_subtasks(self, parent_id: str) -> List[Task]:
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT * FROM tasks WHERE parent_id = ? ORDER BY created_at ASC", (parent_id,)
            ).fetchall()
            return [
                Task(
                    task_id=row["task_id"],
                    prompt=row["prompt"],
                    task_type=row["task_type"],
                    status=TaskStatus(row["status"]),
                    progress=row["progress"],
                    step=row["step"],
                    result=row["result"],
                    error=row["error"],
                    parent_id=row["parent_id"],
                    namespace=row["namespace"] if "namespace" in row.keys() else "default",
                    created_at=row["created_at"],
                    updated_at=row["updated_at"],
                )
                for row in rows
            ]


class PostgreSQLTaskStore(BaseTaskStore):
    """PostgreSQL implementation of the TaskStore interface using connection pooling."""
    
    _pool = None
    
    def __init__(self, connection_url: Optional[str] = None):
        self.connection_url = connection_url
        if not self.connection_url:
            from agyqueue.config import settings
            self.connection_url = settings.database_url
            
        self._init_pool()
        self._init_db()

    def _init_pool(self):
        if PostgreSQLTaskStore._pool is None:
            import psycopg2.pool
            PostgreSQLTaskStore._pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=1,
                maxconn=10,
                dsn=self.connection_url
            )
            logger.info("PostgreSQL thread pool connection manager initialized.")

    @contextmanager
    def _get_conn(self):
        conn = PostgreSQLTaskStore._pool.getconn()
        conn.autocommit = True
        try:
            yield conn
        finally:
            PostgreSQLTaskStore._pool.putconn(conn)

    def _init_db(self):
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS tasks (
                        task_id VARCHAR(50) PRIMARY KEY,
                        prompt TEXT NOT NULL,
                        task_type VARCHAR(50) NOT NULL,
                        status VARCHAR(20) NOT NULL,
                        progress INTEGER NOT NULL,
                        step TEXT NOT NULL,
                        result TEXT,
                        error TEXT,
                        parent_id VARCHAR(50),
                        namespace VARCHAR(50) NOT NULL DEFAULT 'default',
                        created_at VARCHAR(30) NOT NULL,
                        updated_at VARCHAR(30) NOT NULL
                    )
                """)
                try:
                    cur.execute("ALTER TABLE tasks ADD COLUMN namespace VARCHAR(50) NOT NULL DEFAULT 'default';")
                except Exception:
                    pass

    def save_task(self, task: Task) -> None:
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO tasks 
                    (task_id, prompt, task_type, status, progress, step, result, error, parent_id, namespace, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (task_id) DO UPDATE SET
                        prompt = EXCLUDED.prompt,
                        task_type = EXCLUDED.task_type,
                        status = EXCLUDED.status,
                        progress = EXCLUDED.progress,
                        step = EXCLUDED.step,
                        result = EXCLUDED.result,
                        error = EXCLUDED.error,
                        parent_id = EXCLUDED.parent_id,
                        namespace = EXCLUDED.namespace,
                        created_at = EXCLUDED.created_at,
                        updated_at = EXCLUDED.updated_at
                    """,
                    (
                        task.task_id,
                        task.prompt,
                        task.task_type,
                        task.status.value,
                        task.progress,
                        task.step,
                        task.result,
                        task.error,
                        task.parent_id,
                        task.namespace,
                        task.created_at,
                        task.updated_at,
                    ),
                )

    def get_task(self, task_id: str) -> Optional[Task]:
        from psycopg2.extras import RealDictCursor
        with self._get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM tasks WHERE task_id = %s", (task_id,))
                row = cur.fetchone()
                if not row:
                    return None
                return Task(
                    task_id=row["task_id"],
                    prompt=row["prompt"],
                    task_type=row["task_type"],
                    status=TaskStatus(row["status"]),
                    progress=row["progress"],
                    step=row["step"],
                    result=row["result"],
                    error=row["error"],
                    parent_id=row["parent_id"],
                    namespace=row.get("namespace", "default"),
                    created_at=row["created_at"],
                    updated_at=row["updated_at"],
                )

    def update_task(
        self,
        task_id: str,
        status: TaskStatus,
        progress: int,
        step: str,
        result: Optional[str] = None,
        error: Optional[str] = None,
    ) -> Optional[Task]:
        now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE tasks
                    SET status = %s, progress = %s, step = %s, result = COALESCE(%s, result), error = COALESCE(%s, error), updated_at = %s
                    WHERE task_id = %s
                    """,
                    (status.value, progress, step, result, error, now, task_id),
                )
        return self.get_task(task_id)

    def touch_task(self, task_id: str) -> None:
        now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE tasks SET updated_at = %s WHERE task_id = %s",
                    (now, task_id),
                )

    def list_tasks(self, namespace: Optional[str] = None) -> List[Task]:
        from psycopg2.extras import RealDictCursor
        with self._get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                if namespace:
                    cur.execute("SELECT * FROM tasks WHERE namespace = %s ORDER BY created_at DESC", (namespace,))
                else:
                    cur.execute("SELECT * FROM tasks ORDER BY created_at DESC")
                rows = cur.fetchall()
                return [
                    Task(
                        task_id=row["task_id"],
                        prompt=row["prompt"],
                        task_type=row["task_type"],
                        status=TaskStatus(row["status"]),
                        progress=row["progress"],
                        step=row["step"],
                        result=row["result"],
                        error=row["error"],
                        parent_id=row["parent_id"],
                        namespace=row.get("namespace", "default"),
                        created_at=row["created_at"],
                        updated_at=row["updated_at"],
                    )
                    for row in rows
                ]

    def get_subtasks(self, parent_id: str) -> List[Task]:
        from psycopg2.extras import RealDictCursor
        with self._get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT * FROM tasks WHERE parent_id = %s ORDER BY created_at ASC", (parent_id,)
                )
                rows = cur.fetchall()
                return [
                    Task(
                        task_id=row["task_id"],
                        prompt=row["prompt"],
                        task_type=row["task_type"],
                        status=TaskStatus(row["status"]),
                        progress=row["progress"],
                        step=row["step"],
                        result=row["result"],
                        error=row["error"],
                        parent_id=row["parent_id"],
                        namespace=row.get("namespace", "default"),
                        created_at=row["created_at"],
                        updated_at=row["updated_at"],
                    )
                    for row in rows
                ]


def TaskStore(db_path: Optional[str] = None) -> BaseTaskStore:
    """Factory function returning the configured TaskStore implementation.
    
    Can be configured via environment variables (e.g., AGYQUEUE_STORE_TYPE).
    """
    from agyqueue.config import settings
    
    store_type = settings.store_type
    
    # If postgres is set or database_url starts with postgres, use PostgreSQLTaskStore
    if store_type == "postgres" or (settings.database_url and settings.database_url.startswith("postgres")):
        try:
            return PostgreSQLTaskStore()
        except ImportError:
            logger.warning("psycopg2 not installed. Falling back to SQLiteTaskStore.")
            return SQLiteTaskStore(db_path)
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}. Falling back to SQLiteTaskStore.")
            return SQLiteTaskStore(db_path)
            
    return SQLiteTaskStore(db_path)

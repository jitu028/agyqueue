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
        current_attempt: Optional[int] = None,
        max_attempts: Optional[int] = None,
        worker_id: Optional[str] = None,
    ) -> Optional[Task]:
        """Updates the status, progress, current step, and optionally other properties of a task."""
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

    @abstractmethod
    def log_event(self, task_id: str, event_type: str, payload: Optional[str] = None) -> None:
        """Logs a state transition or other key execution event for a task."""
        pass

    @abstractmethod
    def get_events(self, task_id: str) -> List[dict]:
        """Gets chronological history events for a task."""
        pass

    @abstractmethod
    def create_signal(self, task_id: str, signal_name: str, payload: Optional[str] = None) -> str:
        """Registers a signal sent to an active workflow."""
        pass

    @abstractmethod
    def get_signals(self, task_id: str, status: Optional[str] = None) -> List[dict]:
        """Gets all signals received by a task, optionally filtered by status."""
        pass

    @abstractmethod
    def consume_signal(self, signal_id: str) -> None:
        """Marks a signal as consumed by the workflow."""
        pass

    @abstractmethod
    def register_worker(self, worker_id: str, supported_types: str) -> None:
        """Registers or heartbeats a worker node."""
        pass

    @abstractmethod
    def worker_heartbeat(self, worker_id: str) -> None:
        """Updates worker heartbeat timestamp."""
        pass

    @abstractmethod
    def list_active_workers(self) -> List[dict]:
        """Lists active workers that have heartbeated recently."""
        pass

    @abstractmethod
    def save_schedule(self, schedule_id: str, cron_expression: str, workflow_type: str, prompt: str, namespace: str = "default") -> None:
        """Saves a recurring schedule."""
        pass

    @abstractmethod
    def list_schedules(self, active_only: bool = False) -> List[dict]:
        """Lists all schedules."""
        pass

    @abstractmethod
    def update_schedule_run(self, schedule_id: str) -> None:
        """Updates a schedule's last run timestamp."""
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
                    current_attempt INTEGER NOT NULL DEFAULT 1,
                    max_attempts INTEGER NOT NULL DEFAULT 3,
                    worker_id TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    approver_name TEXT,
                    approver_email TEXT
                )
            """)
            try:
                conn.execute("ALTER TABLE tasks ADD COLUMN approver_name TEXT")
            except sqlite3.OperationalError:
                pass
            try:
                conn.execute("ALTER TABLE tasks ADD COLUMN approver_email TEXT")
            except sqlite3.OperationalError:
                pass
            conn.execute("""
                CREATE TABLE IF NOT EXISTS task_events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    payload TEXT,
                    created_at TEXT NOT NULL
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS task_signals (
                    signal_id TEXT PRIMARY KEY,
                    task_id TEXT NOT NULL,
                    signal_name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    payload TEXT,
                    created_at TEXT NOT NULL
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS schedules (
                    schedule_id TEXT PRIMARY KEY,
                    cron_expression TEXT NOT NULL,
                    workflow_type TEXT NOT NULL,
                    prompt TEXT NOT NULL,
                    namespace TEXT NOT NULL DEFAULT 'default',
                    last_run_at TEXT,
                    active INTEGER NOT NULL DEFAULT 1
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS worker_registry (
                    worker_id TEXT PRIMARY KEY,
                    supported_types TEXT NOT NULL,
                    status TEXT NOT NULL,
                    last_heartbeat TEXT NOT NULL
                )
            """)
            conn.commit()
            
            # Schema migrations for existing tasks table
            for col, col_type in [("parent_id", "TEXT"), ("namespace", "TEXT NOT NULL DEFAULT 'default'"), 
                                  ("current_attempt", "INTEGER NOT NULL DEFAULT 1"), ("max_attempts", "INTEGER NOT NULL DEFAULT 3"),
                                  ("worker_id", "TEXT")]:
                try:
                    conn.execute(f"ALTER TABLE tasks ADD COLUMN {col} {col_type};")
                    conn.commit()
                except sqlite3.OperationalError:
                    pass

    def save_task(self, task: Task) -> None:
        with self._get_conn() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO tasks 
                (task_id, prompt, task_type, status, progress, step, result, error, parent_id, namespace, current_attempt, max_attempts, worker_id, created_at, updated_at, approver_name, approver_email)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    task.current_attempt,
                    task.max_attempts,
                    task.worker_id,
                    task.created_at,
                    task.updated_at,
                    task.approver_name,
                    task.approver_email,
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
            
            keys = row.keys()
            return Task(
                task_id=row["task_id"],
                prompt=row["prompt"],
                task_type=row["task_type"],
                status=TaskStatus(row["status"]),
                progress=row["progress"],
                step=row["step"],
                result=row["result"],
                error=row["error"],
                parent_id=row["parent_id"] if "parent_id" in keys else None,
                namespace=row["namespace"] if "namespace" in keys else "default",
                current_attempt=row["current_attempt"] if "current_attempt" in keys else 1,
                max_attempts=row["max_attempts"] if "max_attempts" in keys else 3,
                worker_id=row["worker_id"] if "worker_id" in keys else None,
                created_at=row["created_at"],
                updated_at=row["updated_at"],
                approver_name=row["approver_name"] if "approver_name" in keys else None,
                approver_email=row["approver_email"] if "approver_email" in keys else None,
            )

    def update_task(
        self,
        task_id: str,
        status: TaskStatus,
        progress: int,
        step: str,
        result: Optional[str] = None,
        error: Optional[str] = None,
        current_attempt: Optional[int] = None,
        max_attempts: Optional[int] = None,
        worker_id: Optional[str] = None,
    ) -> Optional[Task]:
        now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
        with self._get_conn() as conn:
            conn.execute(
                """
                UPDATE tasks
                SET status = ?, progress = ?, step = ?, 
                    result = COALESCE(?, result), error = COALESCE(?, error),
                    current_attempt = COALESCE(?, current_attempt),
                    max_attempts = COALESCE(?, max_attempts),
                    worker_id = COALESCE(?, worker_id),
                    updated_at = ?
                WHERE task_id = ?
                """,
                (status.value, progress, step, result, error, current_attempt, max_attempts, worker_id, now, task_id),
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
            
            tasks = []
            for row in rows:
                keys = row.keys()
                tasks.append(Task(
                    task_id=row["task_id"],
                    prompt=row["prompt"],
                    task_type=row["task_type"],
                    status=TaskStatus(row["status"]),
                    progress=row["progress"],
                    step=row["step"],
                    result=row["result"],
                    error=row["error"],
                    parent_id=row["parent_id"] if "parent_id" in keys else None,
                    namespace=row["namespace"] if "namespace" in keys else "default",
                    current_attempt=row["current_attempt"] if "current_attempt" in keys else 1,
                    max_attempts=row["max_attempts"] if "max_attempts" in keys else 3,
                    worker_id=row["worker_id"] if "worker_id" in keys else None,
                    created_at=row["created_at"],
                    updated_at=row["updated_at"],
                ))
            return tasks

    def get_subtasks(self, parent_id: str) -> List[Task]:
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT * FROM tasks WHERE parent_id = ? ORDER BY created_at ASC", (parent_id,)
            ).fetchall()
            
            tasks = []
            for row in rows:
                keys = row.keys()
                tasks.append(Task(
                    task_id=row["task_id"],
                    prompt=row["prompt"],
                    task_type=row["task_type"],
                    status=TaskStatus(row["status"]),
                    progress=row["progress"],
                    step=row["step"],
                    result=row["result"],
                    error=row["error"],
                    parent_id=row["parent_id"] if "parent_id" in keys else None,
                    namespace=row["namespace"] if "namespace" in keys else "default",
                    current_attempt=row["current_attempt"] if "current_attempt" in keys else 1,
                    max_attempts=row["max_attempts"] if "max_attempts" in keys else 3,
                    worker_id=row["worker_id"] if "worker_id" in keys else None,
                    created_at=row["created_at"],
                    updated_at=row["updated_at"],
                ))
            return tasks

    def log_event(self, task_id: str, event_type: str, payload: Optional[str] = None) -> None:
        now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
        with self._get_conn() as conn:
            conn.execute(
                "INSERT INTO task_events (task_id, event_type, payload, created_at) VALUES (?, ?, ?, ?)",
                (task_id, event_type, payload, now)
            )
            conn.commit()

    def get_events(self, task_id: str) -> List[dict]:
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT * FROM task_events WHERE task_id = ? ORDER BY event_id ASC",
                (task_id,)
            ).fetchall()
            return [dict(r) for r in rows]

    def create_signal(self, task_id: str, signal_name: str, payload: Optional[str] = None) -> str:
        import uuid
        signal_id = f"sig-{uuid.uuid4().hex[:8]}"
        now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
        with self._get_conn() as conn:
            conn.execute(
                "INSERT INTO task_signals (signal_id, task_id, signal_name, status, payload, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                (signal_id, task_id, signal_name, "PENDING", payload, now)
            )
            conn.commit()
        return signal_id

    def get_signals(self, task_id: str, status: Optional[str] = None) -> List[dict]:
        with self._get_conn() as conn:
            if status:
                rows = conn.execute(
                    "SELECT * FROM task_signals WHERE task_id = ? AND status = ? ORDER BY created_at ASC",
                    (task_id, status)
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM task_signals WHERE task_id = ? ORDER BY created_at ASC",
                    (task_id,)
                ).fetchall()
            return [dict(r) for r in rows]

    def consume_signal(self, signal_id: str) -> None:
        with self._get_conn() as conn:
            conn.execute(
                "UPDATE task_signals SET status = ? WHERE signal_id = ?",
                ("CONSUMED", signal_id)
            )
            conn.commit()

    def register_worker(self, worker_id: str, supported_types: str) -> None:
        now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
        with self._get_conn() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO worker_registry (worker_id, supported_types, status, last_heartbeat) VALUES (?, ?, ?, ?)",
                (worker_id, supported_types, "ACTIVE", now)
            )
            conn.commit()

    def worker_heartbeat(self, worker_id: str) -> None:
        now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
        with self._get_conn() as conn:
            conn.execute(
                "UPDATE worker_registry SET last_heartbeat = ?, status = 'ACTIVE' WHERE worker_id = ?",
                (now, worker_id)
            )
            conn.commit()

    def list_active_workers(self) -> List[dict]:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        with self._get_conn() as conn:
            rows = conn.execute("SELECT * FROM worker_registry WHERE status = 'ACTIVE'").fetchall()
            active = []
            for r in rows:
                try:
                    last_hb = datetime.fromisoformat(r["last_heartbeat"])
                    if (now - last_hb).total_seconds() < 15.0:
                        active.append(dict(r))
                except Exception:
                    pass
            return active

    def save_schedule(self, schedule_id: str, cron_expression: str, workflow_type: str, prompt: str, namespace: str = "default") -> None:
        with self._get_conn() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO schedules (schedule_id, cron_expression, workflow_type, prompt, namespace, active) VALUES (?, ?, ?, ?, ?, 1)",
                (schedule_id, cron_expression, workflow_type, prompt, namespace)
            )
            conn.commit()

    def list_schedules(self, active_only: bool = False) -> List[dict]:
        with self._get_conn() as conn:
            if active_only:
                rows = conn.execute("SELECT * FROM schedules WHERE active = 1").fetchall()
            else:
                rows = conn.execute("SELECT * FROM schedules").fetchall()
            return [dict(r) for r in rows]

    def update_schedule_run(self, schedule_id: str) -> None:
        now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
        with self._get_conn() as conn:
            conn.execute(
                "UPDATE schedules SET last_run_at = ? WHERE schedule_id = ?",
                (now, schedule_id)
            )
            conn.commit()


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
                        current_attempt INTEGER NOT NULL DEFAULT 1,
                        max_attempts INTEGER NOT NULL DEFAULT 3,
                        worker_id VARCHAR(50),
                        created_at VARCHAR(30) NOT NULL,
                        updated_at VARCHAR(30) NOT NULL,
                        approver_name VARCHAR(100),
                        approver_email VARCHAR(100)
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS task_events (
                        event_id SERIAL PRIMARY KEY,
                        task_id VARCHAR(50) NOT NULL,
                        event_type VARCHAR(50) NOT NULL,
                        payload TEXT,
                        created_at VARCHAR(30) NOT NULL
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS task_signals (
                        signal_id VARCHAR(50) PRIMARY KEY,
                        task_id VARCHAR(50) NOT NULL,
                        signal_name VARCHAR(50) NOT NULL,
                        status VARCHAR(20) NOT NULL,
                        payload TEXT,
                        created_at VARCHAR(30) NOT NULL
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS schedules (
                        schedule_id VARCHAR(50) PRIMARY KEY,
                        cron_expression VARCHAR(50) NOT NULL,
                        workflow_type VARCHAR(50) NOT NULL,
                        prompt TEXT NOT NULL,
                        namespace VARCHAR(50) NOT NULL DEFAULT 'default',
                        last_run_at VARCHAR(30),
                        active INTEGER NOT NULL DEFAULT 1
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS worker_registry (
                        worker_id VARCHAR(50) PRIMARY KEY,
                        supported_types TEXT NOT NULL,
                        status VARCHAR(20) NOT NULL,
                        last_heartbeat VARCHAR(30) NOT NULL
                    )
                """)
                # Migrations
                for col, col_type in [("parent_id", "VARCHAR(50)"), 
                                      ("namespace", "VARCHAR(50) NOT NULL DEFAULT 'default'"),
                                      ("current_attempt", "INTEGER NOT NULL DEFAULT 1"), 
                                      ("max_attempts", "INTEGER NOT NULL DEFAULT 3"),
                                      ("worker_id", "VARCHAR(50)"),
                                      ("approver_name", "VARCHAR(100)"),
                                      ("approver_email", "VARCHAR(100)")]:
                    try:
                        cur.execute(f"ALTER TABLE tasks ADD COLUMN {col} {col_type};")
                    except Exception:
                        pass

    def save_task(self, task: Task) -> None:
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO tasks 
                    (task_id, prompt, task_type, status, progress, step, result, error, parent_id, namespace, current_attempt, max_attempts, worker_id, created_at, updated_at, approver_name, approver_email)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                        current_attempt = EXCLUDED.current_attempt,
                        max_attempts = EXCLUDED.max_attempts,
                        worker_id = EXCLUDED.worker_id,
                        created_at = EXCLUDED.created_at,
                        updated_at = EXCLUDED.updated_at,
                        approver_name = EXCLUDED.approver_name,
                        approver_email = EXCLUDED.approver_email
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
                        task.current_attempt,
                        task.max_attempts,
                        task.worker_id,
                        task.created_at,
                        task.updated_at,
                        task.approver_name,
                        task.approver_email,
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
                    parent_id=row.get("parent_id"),
                    namespace=row.get("namespace", "default"),
                    current_attempt=row.get("current_attempt", 1),
                    max_attempts=row.get("max_attempts", 3),
                    worker_id=row.get("worker_id"),
                    created_at=row["created_at"],
                    updated_at=row["updated_at"],
                    approver_name=row.get("approver_name"),
                    approver_email=row.get("approver_email"),
                )

    def update_task(
        self,
        task_id: str,
        status: TaskStatus,
        progress: int,
        step: str,
        result: Optional[str] = None,
        error: Optional[str] = None,
        current_attempt: Optional[int] = None,
        max_attempts: Optional[int] = None,
        worker_id: Optional[str] = None,
    ) -> Optional[Task]:
        now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE tasks
                    SET status = %s, progress = %s, step = %s, 
                        result = COALESCE(%s, result), error = COALESCE(%s, error),
                        current_attempt = COALESCE(%s, current_attempt),
                        max_attempts = COALESCE(%s, max_attempts),
                        worker_id = COALESCE(%s, worker_id),
                        updated_at = %s
                    WHERE task_id = %s
                    """,
                    (status.value, progress, step, result, error, current_attempt, max_attempts, worker_id, now, task_id),
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
                        parent_id=row.get("parent_id"),
                        namespace=row.get("namespace", "default"),
                        current_attempt=row.get("current_attempt", 1),
                        max_attempts=row.get("max_attempts", 3),
                        worker_id=row.get("worker_id"),
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
                        parent_id=row.get("parent_id"),
                        namespace=row.get("namespace", "default"),
                        current_attempt=row.get("current_attempt", 1),
                        max_attempts=row.get("max_attempts", 3),
                        worker_id=row.get("worker_id"),
                        created_at=row["created_at"],
                        updated_at=row["updated_at"],
                    )
                    for row in rows
                ]

    def log_event(self, task_id: str, event_type: str, payload: Optional[str] = None) -> None:
        now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO task_events (task_id, event_type, payload, created_at) VALUES (%s, %s, %s, %s)",
                    (task_id, event_type, payload, now)
                )

    def get_events(self, task_id: str) -> List[dict]:
        from psycopg2.extras import RealDictCursor
        with self._get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM task_events WHERE task_id = %s ORDER BY event_id ASC", (task_id,))
                return [dict(r) for r in cur.fetchall()]

    def create_signal(self, task_id: str, signal_name: str, payload: Optional[str] = None) -> str:
        import uuid
        signal_id = f"sig-{uuid.uuid4().hex[:8]}"
        now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO task_signals (signal_id, task_id, signal_name, status, payload, created_at) VALUES (%s, %s, %s, %s, %s, %s)",
                    (signal_id, task_id, signal_name, "PENDING", payload, now)
                )
        return signal_id

    def get_signals(self, task_id: str, status: Optional[str] = None) -> List[dict]:
        from psycopg2.extras import RealDictCursor
        with self._get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                if status:
                    cur.execute(
                        "SELECT * FROM task_signals WHERE task_id = %s AND status = %s ORDER BY created_at ASC",
                        (task_id, status)
                    )
                else:
                    cur.execute(
                        "SELECT * FROM task_signals WHERE task_id = %s ORDER BY created_at ASC",
                        (task_id,)
                    )
                return [dict(r) for r in cur.fetchall()]

    def consume_signal(self, signal_id: str) -> None:
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE task_signals SET status = %s WHERE signal_id = %s",
                    ("CONSUMED", signal_id)
                )

    def register_worker(self, worker_id: str, supported_types: str) -> None:
        now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO worker_registry (worker_id, supported_types, status, last_heartbeat) 
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (worker_id) DO UPDATE SET
                        supported_types = EXCLUDED.supported_types,
                        status = EXCLUDED.status,
                        last_heartbeat = EXCLUDED.last_heartbeat
                    """,
                    (worker_id, supported_types, "ACTIVE", now)
                )

    def worker_heartbeat(self, worker_id: str) -> None:
        now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE worker_registry SET last_heartbeat = %s, status = 'ACTIVE' WHERE worker_id = %s",
                    (now, worker_id)
                )

    def list_active_workers(self) -> List[dict]:
        from psycopg2.extras import RealDictCursor
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        with self._get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM worker_registry WHERE status = 'ACTIVE'")
                rows = cur.fetchall()
                active = []
                for r in rows:
                    try:
                        last_hb = datetime.fromisoformat(r["last_heartbeat"])
                        if (now - last_hb).total_seconds() < 15.0:
                            active.append(dict(r))
                    except Exception:
                        pass
                return active

    def save_schedule(self, schedule_id: str, cron_expression: str, workflow_type: str, prompt: str, namespace: str = "default") -> None:
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO schedules (schedule_id, cron_expression, workflow_type, prompt, namespace, active) 
                    VALUES (%s, %s, %s, %s, %s, 1)
                    ON CONFLICT (schedule_id) DO UPDATE SET
                        cron_expression = EXCLUDED.cron_expression,
                        workflow_type = EXCLUDED.workflow_type,
                        prompt = EXCLUDED.prompt,
                        namespace = EXCLUDED.namespace,
                        active = EXCLUDED.active
                    """,
                    (schedule_id, cron_expression, workflow_type, prompt, namespace)
                )

    def list_schedules(self, active_only: bool = False) -> List[dict]:
        from psycopg2.extras import RealDictCursor
        with self._get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                if active_only:
                    cur.execute("SELECT * FROM schedules WHERE active = 1")
                else:
                    cur.execute("SELECT * FROM schedules")
                return [dict(r) for r in cur.fetchall()]

    def update_schedule_run(self, schedule_id: str) -> None:
        now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE schedules SET last_run_at = %s WHERE schedule_id = %s",
                    (now, schedule_id)
                )


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

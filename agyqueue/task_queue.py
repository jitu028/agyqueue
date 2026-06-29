import os
import time
import logging
from abc import ABC, abstractmethod
from typing import Optional
import redis
from agyqueue.storage import TaskStore
from agyqueue.models import TaskStatus

logger = logging.getLogger(__name__)

class BaseTaskQueue(ABC):
    """Abstract base class defining the interface for task queues."""
    
    @abstractmethod
    def enqueue(self, task_id: str) -> None:
        """Pushes a task ID onto the queue."""
        pass

    @abstractmethod
    def dequeue(self, timeout: int = 1) -> Optional[str]:
        """Pops a task ID from the queue. Blocks up to `timeout` seconds if empty."""
        pass


class RedisTaskQueue(BaseTaskQueue):
    """Redis-backed implementation of the TaskQueue interface."""
    
    def __init__(self, redis_url: str):
        self.redis_url = redis_url
        self.redis_client = redis.Redis.from_url(
            self.redis_url, 
            socket_connect_timeout=2.0,
            decode_responses=True
        )
        # Test connection
        self.redis_client.ping()
        logger.info(f"Connected to Redis queue at {self.redis_url}")

    def enqueue(self, task_id: str) -> None:
        self.redis_client.rpush("agyqueue:task_ids", task_id)
        logger.info(f"Enqueued task {task_id} to Redis.")

    def dequeue(self, timeout: int = 1) -> Optional[str]:
        # blpop returns (queue_name, value)
        res = self.redis_client.blpop("agyqueue:task_ids", timeout=timeout)
        if res:
            return res[1]
        return None


class SQLiteTaskQueue(BaseTaskQueue):
    """SQLite-backed polling implementation of the TaskQueue interface."""
    
    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or os.environ.get("AGYQUEUE_DB_PATH", "agyqueue.db")
        self.store = TaskStore(self.db_path)

    def enqueue(self, task_id: str) -> None:
        # SQLite queue relies on the task status being set to QUEUED in the SQLite store
        logger.info(f"Enqueued task {task_id} to SQLite store (waiting for polling worker).")

    def dequeue(self, timeout: int = 1) -> Optional[str]:
        try:
            # We access the internal connection from the sqlite store to perform the dequeue transaction
            # Since the store uses WAL mode, we can claim the task concurrently.
            # Wait, since self.store is a SQLiteTaskStore instance, we can call _get_conn()
            # If the store is abstract, we verify if it has _get_conn
            if hasattr(self.store, "_get_conn"):
                conn = self.store._get_conn()
                with conn:
                    row = conn.execute(
                        "SELECT task_id FROM tasks WHERE status = ? ORDER BY created_at ASC LIMIT 1",
                        (TaskStatus.QUEUED.value,)
                    ).fetchone()
                    
                    if row:
                        task_id = row["task_id"]
                        # Claim the task
                        conn.execute(
                            "UPDATE tasks SET status = ?, step = ?, updated_at = ? WHERE task_id = ?",
                            (
                                TaskStatus.RUNNING.value,
                                "Claimed by worker",
                                time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                                task_id
                            )
                        )
                        return task_id
        except Exception as e:
            logger.error(f"SQLite dequeue failed: {e}")
        
        time.sleep(timeout)
        return None


def TaskQueue(redis_url: Optional[str] = None, db_path: Optional[str] = None) -> BaseTaskQueue:
    """Factory function returning the configured TaskQueue implementation.
    
    Tries to connect to Redis if a URL is provided or set in environment,
    falling back to SQLite if Redis is unavailable.
    """
    url = redis_url or os.environ.get("REDIS_URL")
    if url:
        try:
            return RedisTaskQueue(url)
        except Exception as e:
            logger.warning(f"Could not connect to Redis at {url}: {e}. Falling back to SQLite queue.")
    
    logger.info("Using SQLite database as task queue.")
    return SQLiteTaskQueue(db_path)

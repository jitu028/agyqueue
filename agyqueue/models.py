import json
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional, Any

class TaskStatus(str, Enum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    WAITING = "WAITING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"

@dataclass
class Task:
    task_id: str
    prompt: str
    task_type: str
    status: TaskStatus = TaskStatus.QUEUED
    progress: int = 0
    step: str = "Queued"
    result: Optional[str] = None
    error: Optional[str] = None
    parent_id: Optional[str] = None
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).replace(tzinfo=None).isoformat())
    updated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).replace(tzinfo=None).isoformat())

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Task":
        # Copy to avoid modifying original
        d = dict(data)
        if isinstance(d.get("status"), str):
            d["status"] = TaskStatus(d["status"])
        return cls(**d)

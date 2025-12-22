from dataclasses import dataclass
from typing import Mapping
from enum import Enum

@dataclass(frozen=True)
class Job:
    id: int
    name: str
    tags: Mapping[str, str] | None = None

@dataclass(frozen=True)
class JobRun:
    run_id: int
    job_id: int


class RunStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    CANCELED = "CANCELED"
    UNKNOWN = "UNKNOWN"
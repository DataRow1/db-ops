"""Core domain models for Databricks operations.

This module defines the fundamental data structures used throughout the
application to represent Databricks jobs, job runs, and run states.
These models are intentionally simple, immutable, and free of any
infrastructure or presentation concerns.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Mapping


@dataclass(frozen=True)
class Job:
    """
    Represents a Databricks job.

    Attributes:
        id: Unique identifier of the Databricks job.
        name: Human-readable name of the job.
        tags: Optional mapping of job tags (key-value pairs) as defined
              in Databricks. May be None if no tags are present.
    """

    id: int
    name: str
    tags: Mapping[str, str] | None = None


@dataclass(frozen=True)
class JobRun:
    """
    Represents a single execution (run) of a Databricks job.

    Attributes:
        run_id: Unique identifier of the job run.
        job_id: Identifier of the Databricks job this run belongs to.
    """

    run_id: int
    job_id: int


class RunStatus(str, Enum):
    """
    Enumeration of possible states of a Databricks job run.

    Values:
        PENDING: The run has been created but has not started yet.
        RUNNING: The run is currently executing.
        SUCCESS: The run completed successfully.
        FAILED: The run completed with an error.
        CANCELED: The run was canceled before completion.
        UNKNOWN: The run state could not be determined.
    """

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    CANCELED = "CANCELED"
    UNKNOWN = "UNKNOWN"

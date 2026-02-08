"""Core job domain models plus selection and lookup logic.

This module defines the core job data structures (Job, JobRun, RunStatus)
and the domain-level operations for retrieving and filtering Databricks jobs.
It is intentionally free of CLI concerns (output, prompts, confirmation)
and focuses purely on business logic that can be reused by different
frontends (CLI, automation, tests).
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Mapping, Protocol
from dbops.core.selectors import JobSelector


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


class JobsAdapter(Protocol):
    """Interface for job lookup operations used by the core domain."""

    def find_all_jobs(self) -> list[Job]:
        """Return all jobs visible to the current principal."""
        ...


def select_jobs(adapter: JobsAdapter, selector: JobSelector) -> list[Job]:
    """
    Select jobs using a selector strategy.

    The selector encapsulates matching logic (for example name-based,
    tag-based, or combined selectors). This function iterates over all
    available jobs and applies the selector to each job.

    Args:
        adapter: Databricks jobs adapter used to retrieve all jobs.
        selector: JobSelector instance defining the matching strategy.

    Returns:
        A list of Job objects that match the selector.
    """
    return [job for job in adapter.find_all_jobs() if selector.matches(job)]

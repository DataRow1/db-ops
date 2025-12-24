"""Core job selection and lookup logic.

This module contains domain-level operations for retrieving and filtering
Databricks jobs. It is intentionally free of CLI concerns (output, prompts,
confirmation) and focuses purely on business logic that can be reused by
different frontends (CLI, automation, tests).
"""

from db_ops.core.adapters.databricksjobs import DatabricksJobsAdapter
from db_ops.core.job_models import Job
from db_ops.core.selectors import JobSelector


def find_jobs(adapter: DatabricksJobsAdapter, pattern: str) -> list[Job]:
    """
    Find jobs using a regular expression pattern.

    This is a thin convenience wrapper around the Databricks adapter that
    delegates regex-based matching to the underlying implementation.

    Args:
        adapter: Databricks jobs adapter used to query jobs.
        pattern: Regular expression applied to job names.

    Returns:
        A list of Job objects whose names match the given pattern.
    """
    return adapter.find_jobs_by_regex(pattern)


def select_jobs(
    adapter: DatabricksJobsAdapter,
    selector: JobSelector,
) -> list[Job]:
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

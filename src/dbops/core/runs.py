"""Core job run execution and monitoring logic.

This module contains domain-level functions for starting Databricks jobs
and monitoring their execution status. The functionality here is intentionally
synchronous and infrastructure-agnostic, relying on adapters to communicate
with Databricks while keeping concurrency and polling behavior explicit
and predictable.
"""

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Protocol

from dbops.core.jobs import JobRun, RunStatus


class JobRunsAdapter(Protocol):
    """Interface for starting jobs and querying run status."""

    def start_job(self, job_id: int) -> JobRun:
        """Start a job and return its run handle."""
        ...

    def get_run_status(self, run_id: int) -> RunStatus:
        """Return the current status for a job run."""
        ...


def start_jobs_parallel(
    adapter: JobRunsAdapter,
    job_ids: list[int],
    max_parallel: int,
) -> list[JobRun]:
    """
    Start multiple Databricks jobs in parallel.

    This function uses a thread pool to start multiple jobs concurrently,
    up to the specified maximum level of parallelism. Each job is started
    via the provided Databricks adapter.

    Args:
        adapter: Databricks jobs adapter used to start jobs.
        job_ids: List of Databricks job IDs to start.
        max_parallel: Maximum number of jobs to start concurrently.

    Returns:
        A list of JobRun objects representing the started job runs.
        The order of the returned runs is not guaranteed.
    """
    if max_parallel < 1:
        raise ValueError("max_parallel must be >= 1")
    if not job_ids:
        return []

    runs: list[JobRun] = []

    with ThreadPoolExecutor(max_workers=max_parallel) as pool:
        futures = [pool.submit(adapter.start_job, job_id) for job_id in job_ids]

        for f in as_completed(futures):
            runs.append(f.result())

    return runs


def wait_for_run(
    adapter: JobRunsAdapter,
    run_id: int,
    poll_interval: int = 5,
) -> RunStatus:
    """
    Block until a Databricks job run reaches a terminal state.

    This function polls the Databricks API at a fixed interval until the
    run reaches a terminal status (SUCCESS, FAILED, or CANCELED).

    Args:
        adapter: Databricks jobs adapter used to query run status.
        run_id: Identifier of the Databricks job run to monitor.
        poll_interval: Time in seconds to wait between status checks.

    Returns:
        The final RunStatus of the job run.
    """
    while True:
        status = adapter.get_run_status(run_id)
        if status in {
            RunStatus.SUCCESS,
            RunStatus.FAILED,
            RunStatus.CANCELED,
        }:
            return status

        time.sleep(poll_interval)

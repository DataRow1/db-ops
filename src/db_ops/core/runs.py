import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from db_ops.core.models import JobRun, RunStatus
from db_ops.core.adapters.databricks import DatabricksJobsAdapter


def start_jobs_parallel(
    adapter: DatabricksJobsAdapter,
    job_ids: list[int],
    max_parallel: int,
) -> list[JobRun]:
    runs: list[JobRun] = []

    with ThreadPoolExecutor(max_workers=max_parallel) as pool:
        futures = [
            pool.submit(adapter.start_job, job_id)
            for job_id in job_ids
        ]

        for f in as_completed(futures):
            runs.append(f.result())

    return runs


def wait_for_run(
    adapter: DatabricksJobsAdapter,
    run_id: int,
    poll_interval: int = 5,
) -> RunStatus:
    while True:
        status = adapter.get_run_status(run_id)
        if status in {
            RunStatus.SUCCESS,
            RunStatus.FAILED,
            RunStatus.CANCELED,
        }:
            return status

        time.sleep(poll_interval)
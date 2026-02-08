import pytest

from dbops.core.jobs import JobRun
from dbops.core.runs import start_jobs_parallel


class _RunsAdapterStub:
    def start_job(self, job_id: int) -> JobRun:
        return JobRun(run_id=job_id * 10, job_id=job_id)


def test_start_jobs_parallel_rejects_non_positive_parallel():
    with pytest.raises(ValueError, match="max_parallel"):
        start_jobs_parallel(_RunsAdapterStub(), [1], 0)


def test_start_jobs_parallel_returns_empty_on_empty_input():
    assert start_jobs_parallel(_RunsAdapterStub(), [], 2) == []


def test_start_jobs_parallel_starts_all_jobs():
    runs = start_jobs_parallel(_RunsAdapterStub(), [1, 2, 3], 2)

    assert sorted((r.job_id, r.run_id) for r in runs) == [
        (1, 10),
        (2, 20),
        (3, 30),
    ]

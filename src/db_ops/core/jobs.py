from db_ops.core.models import Job
from db_ops.core.adapters.databricks import DatabricksJobsAdapter
from db_ops.core.selectors import JobSelector
from db_ops.core.models import Job
from db_ops.core.adapters.databricks import DatabricksJobsAdapter


def find_jobs(adapter: DatabricksJobsAdapter, pattern: str) -> list[Job]:
    return adapter.find_jobs_by_regex(pattern)


def select_jobs(
    adapter: DatabricksJobsAdapter,
    selector: JobSelector,
) -> list[Job]:
    return [
        job
        for job in adapter.find_all_jobs()
        if selector.matches(job)
    ]
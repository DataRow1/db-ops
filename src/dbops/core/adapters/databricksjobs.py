from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunResultState

from dbops.core.jobs import Job, JobRun, RunStatus


class DatabricksJobsAdapter:
    def __init__(self, client: WorkspaceClient):
        self.client = client

    def find_all_jobs(self) -> list[Job]:
        jobs: list[Job] = []

        for j in self.client.jobs.list():
            if not j.settings or not j.settings.name:
                continue

            jobs.append(
                Job(
                    id=j.job_id,
                    name=j.settings.name,
                    tags=j.settings.tags or {},
                )
            )

        return jobs

    def start_job(self, job_id: int) -> JobRun:
        run = self.client.jobs.run_now(job_id=job_id)
        return JobRun(run_id=run.run_id, job_id=job_id)

    def get_run_status(self, run_id: int) -> RunStatus:
        run = self.client.jobs.get_run(run_id)
        state = run.state

        if not state:
            return RunStatus.UNKNOWN

        if state.result_state == RunResultState.SUCCESS:
            return RunStatus.SUCCESS
        if state.result_state == RunResultState.FAILED:
            return RunStatus.FAILED
        if state.result_state == RunResultState.CANCELED:
            return RunStatus.CANCELED

        if state.life_cycle_state:
            return RunStatus.RUNNING

        return RunStatus.UNKNOWN

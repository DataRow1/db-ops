import questionary
from databricks.sdk.service.jobs import BaseJob

def select_jobs(jobs: list[BaseJob]) -> list[BaseJob]:
    return questionary.checkbox(
        "Selecteer jobs:",
        choices=[
            questionary.Choice(
                title=f"{job.job_id} – {job.settings.name}",
                value=job,
            )
            for job in jobs
        ],
    ).ask()
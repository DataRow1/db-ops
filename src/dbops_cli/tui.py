import questionary
from db_ops.core.models import Job


def select_jobs(jobs: list[Job]) -> list[Job]:
    choices = [
        questionary.Choice(
            title=f"{job.id} – {job.name}",
            value=job,
        )
        for job in jobs
    ]

    return questionary.checkbox(
        "Select jobs:",
        choices=choices,
    ).ask() or []
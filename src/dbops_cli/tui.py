"""Terminal UI utilities for Databricks operations tooling."""

import questionary

from db_ops.core.job_models import Job
from dbops_cli.common.tui_style import QUESTIONARY_STYLE


def select_jobs(jobs: list[Job]) -> list[Job]:
    """Display a checkbox prompt to select jobs from a list.

    Args:
        jobs: A list of Job objects to choose from.

    Returns:
        A list of selected Job objects, or an empty list if none selected.
    """
    choices = [
        questionary.Choice(
            title=f"{job.id} – {job.name}",
            value=job,
        )
        for job in jobs
    ]

    return (
        questionary.checkbox(
            "Select jobs:",
            choices=choices,
            style=QUESTIONARY_STYLE,
        ).ask()
        or []
    )

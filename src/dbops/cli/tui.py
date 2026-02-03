"""Terminal UI utilities for Databricks operations tooling."""

import questionary

from dbops.core.jobs import Job
from dbops.cli.common.tui_style import QUESTIONARY_STYLE_SELECT


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
            style=QUESTIONARY_STYLE_SELECT,
        ).ask()
        or []
    )

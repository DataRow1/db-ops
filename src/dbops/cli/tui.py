"""Terminal UI utilities for Databricks operations tooling."""

from __future__ import annotations

import questionary

from dbops.cli.common.tui_style import QUESTIONARY_STYLE_SELECT
from dbops.core.jobs import Job

_MAX_JOB_NAME_WIDTH = 96


def _truncate(text: str, max_len: int) -> str:
    """Return text capped at max_len characters using an ASCII ellipsis."""
    if max_len <= 3 or len(text) <= max_len:
        return text[:max_len]
    return f"{text[: max_len - 3]}..."


def _job_choice_title(job: Job, *, name_width: int) -> str:
    """Format one job choice as `<name>  (id: <job_id>)` with aligned id column."""
    short_name = _truncate(job.name, _MAX_JOB_NAME_WIDTH)
    return f"{short_name.ljust(name_width)}  (id: {job.id})"


def select_jobs(jobs: list[Job]) -> list[Job]:
    """Display a checkbox prompt to select jobs from a list.

    Args:
        jobs: A list of Job objects to choose from.

    Returns:
        A list of selected Job objects, or an empty list if none selected.
    """
    shown_names = [_truncate(job.name, _MAX_JOB_NAME_WIDTH) for job in jobs]
    name_width = max((len(name) for name in shown_names), default=0)

    choices = [
        questionary.Choice(
            title=_job_choice_title(job, name_width=name_width),
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

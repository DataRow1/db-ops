"""Progress formatting utilities for the CLI."""

from __future__ import annotations

import time
from typing import Mapping

from rich.console import Console, Group
from rich.live import Live
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
)

from dbops.core.jobs import JobRun, RunStatus
from dbops.core.runs import JobRunsAdapter

console = Console()
_MAX_JOB_NAME_WIDTH = 56


def _truncate(text: str, max_len: int) -> str:
    """Return text capped at max_len characters using an ASCII ellipsis."""
    if max_len <= 3 or len(text) <= max_len:
        return text[:max_len]
    return f"{text[: max_len - 3]}..."


def _display_job_label(
    job_id: int,
    job_name_by_id: Mapping[int, str] | None,
    *,
    name_width: int,
) -> str:
    """
    Render a job label for the live progress list.

    - With mapping: `<name>  (id: <id>)` with aligned id column.
    - Without mapping (or if missing): fallback to just `<id>`.
    """
    if not job_name_by_id:
        return str(job_id)

    raw_name = job_name_by_id.get(job_id)
    if raw_name is None:
        return str(job_id)

    name = str(raw_name)
    if name == str(job_id):
        return str(job_id)

    short_name = _truncate(name, _MAX_JOB_NAME_WIDTH)
    return f"{short_name.ljust(name_width)}  (id: {job_id})"


def wait_for_runs_with_progress(
    adapter: JobRunsAdapter,
    runs: list[JobRun],
    poll_interval: int = 5,
    job_name_by_id: Mapping[int, str] | None = None,
) -> list[tuple[JobRun, RunStatus]]:
    """
    Poll all runs until they reach a terminal state. Shows:
      - an overall progress bar (x/y completed + failures)
      - per-run spinner rows with elapsed timers (stops per run when finished)

    Returns list of (JobRun, RunStatus).
    """
    statuses: dict[int, RunStatus] = {r.run_id: RunStatus.PENDING for r in runs}
    finished: set[int] = set()
    failures = 0
    shown_job_names = [
        _truncate(str(name), _MAX_JOB_NAME_WIDTH)
        for r in runs
        for name in [job_name_by_id.get(r.job_id) if job_name_by_id else None]
        if name is not None and str(name) != str(r.job_id)
    ]
    name_width = max((len(name) for name in shown_job_names), default=0)

    def _style_for(status: RunStatus) -> str:
        if status == RunStatus.SUCCESS:
            return "green"
        if status in (RunStatus.FAILED, RunStatus.CANCELED):
            return "red"
        if status in (RunStatus.RUNNING, RunStatus.PENDING):
            return "yellow"
        return "dim"

    # Overall bar (no per-run fields here)
    overall = Progress(
        TextColumn("[bold]Overall[/]"),
        BarColumn(),
        TaskProgressColumn(),  # e.g. 2/5
        TextColumn("failures=[bold red]{task.fields[failures]}[/]"),
        TimeElapsedColumn(),
        console=console,
    )

    # Per-run rows (expects job/run_id/status fields)
    per_run = Progress(
        SpinnerColumn(),
        TextColumn("[bold]{task.fields[job]}[/]"),
        TextColumn("run_id={task.fields[run_id]}"),
        TextColumn(
            "status=[{task.fields[style]}]{task.fields[status]}[/{task.fields[style]}]"
        ),
        TimeElapsedColumn(),
        console=console,
    )

    overall_task_id = overall.add_task(
        "overall",
        total=max(len(runs), 1),
        failures=0,  # ✅ field used by the failures column
    )

    task_ids: dict[int, int] = {}
    for r in runs:
        job_label = _display_job_label(
            r.job_id,
            job_name_by_id,
            name_width=name_width,
        )
        task_ids[r.run_id] = per_run.add_task(
            "",
            total=1,  # finite => elapsed stops when completed
            job=job_label,
            run_id=str(r.run_id),
            status="PENDING",
            style=_style_for(RunStatus.PENDING),
        )

    group = Group(overall, per_run)

    with Live(group, console=console, refresh_per_second=10, transient=True):
        while len(finished) < len(runs):
            for r in runs:
                if r.run_id in finished:
                    continue

                st = adapter.get_run_status(r.run_id)
                statuses[r.run_id] = st

                per_run.update(
                    task_ids[r.run_id],
                    status=st.value,
                    style=_style_for(st),
                )

                if st in (RunStatus.SUCCESS, RunStatus.FAILED, RunStatus.CANCELED):
                    finished.add(r.run_id)

                    # Count failures (FAILED or CANCELED)
                    if st in (RunStatus.FAILED, RunStatus.CANCELED):
                        failures += 1
                        overall.update(overall_task_id, failures=failures)

                    done_label = "DONE" if st == RunStatus.SUCCESS else st.value

                    per_run.update(
                        task_ids[r.run_id],
                        status=done_label,
                        style=_style_for(st),
                        completed=1,  # stops spinner + freezes elapsed
                    )

                    overall.advance(overall_task_id, 1)

            time.sleep(poll_interval)

        overall.update(overall_task_id, completed=len(runs))

    return [(r, statuses[r.run_id]) for r in runs]

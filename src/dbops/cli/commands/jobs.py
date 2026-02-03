"""Commands for managing Databricks Jobs."""

import typer
from rich.prompt import Confirm

from dbops.core.jobs import RunStatus
from dbops.core.jobs import select_jobs as core_select_jobs
from dbops.core.runs import start_jobs_parallel
from dbops.cli.common.selector_builder import build_selector
from dbops.cli.common.context import AppContext, build_context
from dbops.cli.common.exits import die, ok_exit, warn_exit
from dbops.cli.common.options import (
    ConfirmOpt,
    DryRunOpt,
    NameOpt,
    ParallelOpt,
    ProfileOpt,
    TagOpt,
    UseOrOpt,
    WatchOpt,
)
from dbops.cli.common.output import out
from dbops.cli.common.progress import wait_for_runs_with_progress
from dbops.cli.tui import select_jobs as tui_select_jobs

app = typer.Typer(help="Work with Databricks Jobs", no_args_is_help=True)


@app.callback()
def _init(
    ctx: typer.Context,
    profile: str | None = ProfileOpt,
):
    # Build shared context (client + adapter) once per invocation
    ctx.obj = build_context(profile)


@app.command()
def find(
    ctx: typer.Context,
    name: str | None = NameOpt,
    tag: list[str] = TagOpt,
    use_or: bool = UseOrOpt,
):
    """
    Find jobs using selectors.
    """
    appctx: AppContext = ctx.obj

    try:
        selector = build_selector(name=name, tags=tag, use_or=use_or)
    except ValueError as e:
        die(str(e), code=1)

    jobs = core_select_jobs(appctx.adapter, selector)

    if not jobs:
        warn_exit("No jobs found", code=0)

    out.jobs_table(jobs, title="Matched jobs")


@app.command()
def run(
    ctx: typer.Context,
    name: str | None = NameOpt,
    tag: list[str] = TagOpt,
    use_or: bool = UseOrOpt,
    parallel: int = ParallelOpt,
    confirm: bool = ConfirmOpt,
    watch: bool = WatchOpt,
    dry_run: bool = DryRunOpt,
):
    """
    Run jobs using selectors.
    """
    appctx: AppContext = ctx.obj

    try:
        selector = build_selector(name=name, tags=tag, use_or=use_or)
    except ValueError as e:
        die(str(e), code=1)

    jobs = core_select_jobs(appctx.adapter, selector)

    if not jobs:
        warn_exit("No jobs found", code=0)

    selected = tui_select_jobs(jobs)

    if not selected:
        warn_exit("No jobs selected", code=0)

    out.header("Selected jobs")
    out.jobs_table(selected, title="Selected")

    # ✅ Dry-run output (standardized)
    if dry_run:
        warn_exit("Dry-run enabled: no jobs were started", code=0)

    if confirm and not Confirm.ask("Start the selected jobs?"):
        ok_exit("Cancelled")

    runs = start_jobs_parallel(appctx.adapter, [j.id for j in selected], parallel)

    out.success(f"Jobs started: {len(runs)} run(s)")
    out.runs_table(runs, title="Started runs")

    if watch:
        results = wait_for_runs_with_progress(appctx.adapter, runs, poll_interval=5)

        out.run_status_table(results, title="Run status")

        failed = any(status != RunStatus.SUCCESS for _, status in results)
        if failed:
            raise typer.Exit(2)

import typer
from rich import print
from rich.prompt import Confirm

from db_ops.core.auth import get_client
from db_ops.core.adapters.databricks import DatabricksJobsAdapter
from db_ops.core.jobs import select_jobs
from db_ops.core.runs import start_jobs_parallel, wait_for_run
from db_ops.core.models import RunStatus

from dbops_cli.tui import select_jobs as tui_select_jobs
from db_ops.core.selectors import (
    JobSelector,
    NameRegexSelector,
    TagSelector,
    AndSelector,
    OrSelector,
)
from db_ops.core.selector_builder import build_selector

app = typer.Typer(
    help="dbops-cli – Databricks operations tooling",
    no_args_is_help=True,
)


@app.command()
def find(
    name: str | None = typer.Option(
        None, "--name", help="Regex on job name"
    ),
    tag: list[str] = typer.Option(
        [], "--tag", help="Tag selector (key=value)", show_default=False
    ),
    use_or: bool = typer.Option(
        False, "--or", help="Use OR instead of AND"
    ),
    profile: str | None = typer.Option(
        None, "--profile", "-p", help="Specify a Databricks CLI profile"
    ),
):
    """
    Find Databricks jobs with selectors
    """
    try:
        selector = build_selector(name=name, tags=tag, use_or=use_or)
    except ValueError as e:
        print(f"[red]{e}[/]")
        raise typer.Exit(1)

    client = get_client(profile)
    adapter = DatabricksJobsAdapter(client)

    jobs = select_jobs(adapter, selector)

    if not jobs:
        print("[yellow]Unable to find jobs[/]")
        raise typer.Exit(0)

    for job in jobs:
        tags = ", ".join(f"{k}={v}" for k, v in (job.tags or {}).items())
        print(f"[green]{job.id}[/]  {job.name}  [dim]{tags}[/]")


@app.command()
def run(
    name: str | None = typer.Option(
        None, "--name", help="Regex on job name"
    ),
    tag: list[str] = typer.Option(
        [], "--tag", help="Tag selector (key=value)", show_default=False
    ),
    use_or: bool = typer.Option(
        False, "--or", help="Use OR instead of AND"
    ),
    profile: str | None = typer.Option(
        None, "--profile", "-p", help="Specify a Databricks CLI profile"
    ),
    parallel: int = typer.Option(
        5, "--parallel", "-n", help="Start jobs in parallel"
    ),
    confirm: bool = typer.Option(
        True, "--confirm/--no-confirm", help="Ask for confirmation before starting jobs"
    ),
    watch: bool = typer.Option(
        False, "--watch", "-w", help="Wait for jobs to complete"
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Show which jobs would be started, but don't start them"
    ),
):
    """
    Start Databricks jobs with selectors.
    """
    try:
        selector = build_selector(name=name, tags=tag, use_or=use_or)
    except ValueError as e:
        print(f"[red]{e}[/]")
        raise typer.Exit(1)

    client = get_client(profile)
    adapter = DatabricksJobsAdapter(client)

    jobs = select_jobs(adapter, selector)

    if not jobs:
        print("[yellow]nable to find jobs[/]")
        raise typer.Exit(0)

    selected = tui_select_jobs(jobs)

    if not selected:
        print("[yellow]No jobs selected[/]")
        raise typer.Exit(0)

    print("[bold]Selected jobs:[/]")
    for job in selected:
        tags = ", ".join(f"{k}={v}" for k, v in (job.tags or {}).items())
        print(f"  [green]{job.id}[/]  {job.name}  [dim]{tags}[/]")

    if dry_run:
        print("\n[yellow]Dry-run active: no jobs started[/]")
        raise typer.Exit(0)

    if confirm and not Confirm.ask("Start selected jobs?"):
        raise typer.Exit(0)

    job_ids = [job.id for job in selected]

    runs = start_jobs_parallel(adapter, job_ids, parallel)

    print("\n[green]Jobs started:[/]")
    for run in runs:
        print(f"  job_id={run.job_id}  run_id={run.run_id}")

    if watch:
        failed = False

        for run in runs:
            status = wait_for_run(adapter, run.run_id)
            print(f"Run {run.run_id}: {status}")

            if status != RunStatus.SUCCESS:
                failed = True

        if failed:
            raise typer.Exit(2)


if __name__ == "__main__":
    app()
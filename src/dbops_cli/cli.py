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
        None, "--name", help="Regex op jobnaam"
    ),
    tag: list[str] = typer.Option(
        [], "--tag", help="Tag selector (key=value)", show_default=False
    ),
    use_or: bool = typer.Option(
        False, "--or", help="Gebruik OR i.p.v. AND"
    ),
    profile: str | None = typer.Option(
        None, "--profile", "-p", help="Databricks CLI profile"
    ),
):
    """
    Zoek Databricks jobs met selectors.
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
        print("[yellow]Geen jobs gevonden[/]")
        raise typer.Exit(0)

    for job in jobs:
        tags = ", ".join(f"{k}={v}" for k, v in (job.tags or {}).items())
        print(f"[green]{job.id}[/]  {job.name}  [dim]{tags}[/]")


@app.command()
def run(
    name: str | None = typer.Option(
        None, "--name", help="Regex op jobnaam"
    ),
    tag: list[str] = typer.Option(
        [], "--tag", help="Tag selector (key=value)", show_default=False
    ),
    use_or: bool = typer.Option(
        False, "--or", help="Gebruik OR i.p.v. AND"
    ),
    profile: str | None = typer.Option(
        None, "--profile", "-p", help="Databricks CLI profile"
    ),
    parallel: int = typer.Option(
        5, "--parallel", "-n", help="Aantal jobs parallel starten"
    ),
    confirm: bool = typer.Option(
        True, "--confirm/--no-confirm", help="Bevestiging vragen"
    ),
    watch: bool = typer.Option(
        False, "--watch", "-w", help="Wacht tot runs klaar zijn"
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Toon welke jobs zouden starten, maar start niets"
    ),
):
    """
    Start Databricks jobs met selectors.
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
        print("[yellow]Geen jobs gevonden[/]")
        raise typer.Exit(0)

    selected = tui_select_jobs(jobs)

    if not selected:
        print("[yellow]Geen jobs geselecteerd[/]")
        raise typer.Exit(0)

    print("[bold]Geselecteerde jobs:[/]")
    for job in selected:
        tags = ", ".join(f"{k}={v}" for k, v in (job.tags or {}).items())
        print(f"  [green]{job.id}[/]  {job.name}  [dim]{tags}[/]")

    if dry_run:
        print("\n[yellow]Dry-run actief: geen jobs gestart[/]")
        raise typer.Exit(0)

    if confirm and not Confirm.ask("Geselecteerde jobs starten?"):
        raise typer.Exit(0)

    job_ids = [job.id for job in selected]

    runs = start_jobs_parallel(adapter, job_ids, parallel)

    print("\n[green]Jobs gestart:[/]")
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
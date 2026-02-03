"""Common CLI options for the CLI."""

import typer

ProfileOpt = typer.Option(
    None,
    "--profile",
    "-p",
    help="Databricks CLI profile (from ~/.databrickscfg)",
)

NameOpt = typer.Option(
    None,
    "--name",
    help="Regex on job name",
)

TagOpt = typer.Option(
    [],
    "--tag",
    help="Tag selector (key=value). This is reusable.",
    show_default=False,
)

UseOrOpt = typer.Option(
    False,
    "--or",
    help="Use OR instead of AND between selectors",
)

ParallelOpt = typer.Option(
    5,
    "--parallel",
    "-n",
    help="Number of jobs to start in parallel",
)

ConfirmOpt = typer.Option(
    True,
    "--confirm/--no-confirm",
    help="Ask for confirmation before starting jobs",
)

WatchOpt = typer.Option(
    False,
    "--watch",
    "-w",
    help="Wait until runs are complete",
)

DryRunOpt = typer.Option(
    False,
    "--dry-run",
    help="Show which jobs would start, but don't start anything",
)

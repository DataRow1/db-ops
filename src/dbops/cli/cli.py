"""CLI application for Databricks operations tooling."""

import typer

from dbops.cli.commands.jobs import app as jobs_app
from dbops.cli.commands.unitycatalog import uc_app
from dbops.cli.common.banner import opt_print_banner

opt_print_banner()

app = typer.Typer(
    help="dbops-cli - Databricks operations tooling",
    no_args_is_help=True,
)

app.add_typer(jobs_app, name="jobs", help="Search / start / monitor Databricks jobs.")
app.add_typer(uc_app, name="uc")


if __name__ == "__main__":
    app()

from __future__ import annotations

import re

import typer
from databricks.sdk.errors import NotFound, PermissionDenied

from db_ops.core.adapters.unitycatalog import UnityCatalogAdapter
from db_ops.core.auth import get_client
from db_ops.core.catalog import (
    delete_schema_with_tables,
    delete_tables,
    parse_schema_full_name,
)
from dbops_cli.common.options import ProfileOpt
from dbops_cli.common.output import out

uc_app = typer.Typer(help="Unity Catalog operations.", no_args_is_help=True)


@uc_app.command("tables-delete")
def tables_delete(
    schema_arg: str | None = typer.Argument(
        None, help="Schema in the form catalog.schema"
    ),
    schema: str | None = typer.Option(
        None, "--schema", help="Schema in the form catalog.schema"
    ),
    name: str | None = typer.Option(
        None, "--name", help="Regex filter for table full names"
    ),
    all_: bool = typer.Option(
        False, "--all", help="Delete all matched tables without selection UI"
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Show what would be deleted, but do nothing"
    ),
    yes: bool = typer.Option(False, "--yes", help="Skip confirmation prompt"),
    profile: str | None = ProfileOpt,
):
    """Delete one or more Unity Catalog tables (owner -> delete)."""
    client = get_client(profile)
    adapter = UnityCatalogAdapter(client)

    schema_full_name = schema or schema_arg
    if not schema_full_name:
        out.error(
            "Missing schema. Provide it as a positional argument or via --schema."
        )
        raise typer.Exit(2)

    catalog, schema_name = parse_schema_full_name(schema_full_name)
    try:
        tables = adapter.list_tables(catalog=catalog, schema=schema_name)
    except NotFound:
        out.error(f"Schema '{catalog}.{schema_name}' does not exist.")
        raise typer.Exit(1)
    except PermissionDenied:
        out.error(f"No permission to access schema '{catalog}.{schema_name}'.")
        raise typer.Exit(1)

    full_names = [t.full_name for t in tables]

    if name:
        rx = re.compile(name)
        full_names = [n for n in full_names if rx.search(n)]

    if not full_names:
        out.warn("No tables found.")
        raise typer.Exit(0)

    out.header("Matched tables")
    out.tables_table(full_names, title="Matched tables")

    selected = (
        full_names if all_ else out.select_many("Select tables to delete:", full_names)
    )

    if not selected:
        out.warn("No tables selected.")
        raise typer.Exit(0)

    out.header("Selected tables")
    out.tables_table(selected, title="Selected tables")

    out.info(f"Matched: {len(full_names)} | Selected: {len(selected)}")
    if dry_run:
        out.warn("DRY RUN: no changes will be made.")

    if not yes and not dry_run:
        if not out.confirm("Proceed with deleting the selected tables?"):
            out.warn("Cancelled.")
            raise typer.Exit(0)

    results = delete_tables(adapter, selected, dry_run=dry_run)

    out.uc_delete_results_table(results, title="Delete results")

    failed = [r for r in results if getattr(r, "error", None)]
    if failed:
        out.error(f"Failed to delete {len(failed)} table(s).")
        raise typer.Exit(1)

    out.success(f"Deleted {len(results)} table(s).")


@uc_app.command("schema-delete")
def schema_delete(
    schema: str = typer.Argument(..., help="Schema in the form catalog.schema"),
    name: str | None = typer.Option(
        None, "--name", help="Optional regex to select a subset of tables"
    ),
    force: bool = typer.Option(
        False, "--force", help="Force schema deletion even if not empty"
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Show what would be deleted, but do nothing"
    ),
    yes: bool = typer.Option(False, "--yes", help="Skip confirmation prompt"),
    profile: str | None = ProfileOpt,
):
    """Delete schema after taking ownership and deleting tables within it."""
    client = get_client(profile)
    adapter = UnityCatalogAdapter(client)

    if "." not in schema:
        out.error("Schema must be in the form catalog.schema")
        raise typer.Exit(2)

    try:
        plan = delete_schema_with_tables(
            adapter,
            schema_full_name=schema,
            table_name_regex=name,
            force_schema_delete=force,
            dry_run=True,
        )
    except NotFound:
        out.error(f"Schema '{schema}' does not exist.")
        raise typer.Exit(1)
    except PermissionDenied:
        out.error(f"No permission to access schema '{schema}'.")
        raise typer.Exit(1)

    out.header("Deletion plan")
    out.kv(
        {
            "Schema": plan["schema"],
            "Owner (will become)": plan["owner"],
            "Tables to delete": len(plan["tables"]),
        }
    )
    out.tables_table(plan["tables"], title="Tables to delete")

    if dry_run:
        out.warn("DRY RUN: no changes will be made.")
        raise typer.Exit(0)

    if not yes:
        if not out.confirm("Proceed with deleting tables and then the schema?"):
            out.warn("Cancelled.")
            raise typer.Exit(0)

    delete_schema_with_tables(
        adapter,
        schema_full_name=schema,
        table_name_regex=name,
        force_schema_delete=force,
        dry_run=False,
    )

    out.success("Schema deletion completed.")

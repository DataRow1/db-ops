from __future__ import annotations

import re

import typer
from databricks.sdk.errors import NotFound, PermissionDenied

from dbops.core.catalog import (
    delete_schema_with_tables,
    delete_tables,
    drop_empty_schemas,
    find_empty_schemas,
    parse_schema_full_name,
    set_tables_owner,
)
from dbops.cli.common.context import UCAppContext, build_uc_context
from dbops.cli.common.exits import exit_from_exc
from dbops.cli.common.options import ProfileOpt
from dbops.cli.common.output import out

uc_app = typer.Typer(
    help="Unity Catalog operations.",
    no_args_is_help=False,
    invoke_without_command=True,
)


@uc_app.callback()
def _init(ctx: typer.Context, profile: str | None = ProfileOpt):
    """Initialize Unity Catalog context."""
    ctx.obj = build_uc_context(profile)
    if ctx.invoked_subcommand is None:
        typer.echo(ctx.get_help())
        raise typer.Exit(0)


@uc_app.command("tables-list")
def tables_list(
    ctx: typer.Context,
    schema_arg: str | None = typer.Argument(
        None, help="Schema in the form catalog.schema"
    ),
    schema: str | None = typer.Option(
        None, "--schema", help="Schema in the form catalog.schema"
    ),
    name: str | None = typer.Option(
        None, "--name", help="Regex filter for table full names"
    ),
    owner: str | None = typer.Option(None, "--owner", help="Filter by table owner"),
    type_: str | None = typer.Option(
        None,
        "--type",
        help="Filter by table type (e.g. MANAGED, EXTERNAL, VIEW). Case-insensitive.",
    ),
):
    """List Unity Catalog tables in a schema."""
    appctx: UCAppContext = ctx.obj
    adapter = appctx.adapter

    schema_full_name = schema or schema_arg
    if not schema_full_name:
        out.error(
            "Missing schema. Provide it as a positional argument or via --schema."
        )
        raise typer.Exit(2)

    catalog, schema_name = parse_schema_full_name(schema_full_name)

    try:
        with out.status("Loading tables..."):
            tables = adapter.list_tables(catalog=catalog, schema=schema_name)
    except NotFound as exc:
        exit_from_exc(
            exc, message=f"Schema '{schema_full_name}' does not exist.", code=1
        )
    except PermissionDenied as exc:
        exit_from_exc(
            exc, message=f"No permission to access schema '{schema_full_name}'.", code=1
        )

    if name:
        rx = re.compile(name)
        tables = [t for t in tables if rx.search(t.full_name)]

    if owner:
        tables = [t for t in tables if (t.owner or "").lower() == owner.lower()]

    if type_:
        want = type_.lower()
        tables = [t for t in tables if (t.table_type or "").lower() == want]

    if not tables:
        out.warn("No tables found.")
        raise typer.Exit(0)

    out.header("Tables")
    out.info(f"Schema: {schema_full_name} | Tables: {len(tables)}")
    out.tables_table(tables, title="Tables")


@uc_app.command("tables-owner-set")
def tables_owner_set(
    ctx: typer.Context,
    schema_arg: str | None = typer.Argument(
        None, help="Schema in the form catalog.schema"
    ),
    schema: str | None = typer.Option(
        None, "--schema", help="Schema in the form catalog.schema"
    ),
    name: str | None = typer.Option(
        None, "--name", help="Regex filter for table full names"
    ),
    owner: str = typer.Option(
        ..., "--owner", help="New table owner (user or service principal)"
    ),
    all_: bool = typer.Option(
        False, "--all", help="Change owner for all matched tables without selection UI"
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Show what would change, but do nothing"
    ),
    yes: bool = typer.Option(False, "--yes", help="Skip confirmation prompt"),
):
    """Set the owner for one or more Unity Catalog tables."""
    appctx: UCAppContext = ctx.obj
    adapter = appctx.adapter

    schema_full_name = schema or schema_arg
    if not schema_full_name:
        out.error(
            "Missing schema. Provide it as a positional argument or via --schema."
        )
        raise typer.Exit(2)

    catalog, schema_name = parse_schema_full_name(schema_full_name)
    try:
        with out.status("Loading tables..."):
            tables = adapter.list_tables(catalog=catalog, schema=schema_name)
    except NotFound as exc:
        exit_from_exc(
            exc, message=f"Schema '{schema_full_name}' does not exist.", code=1
        )
    except PermissionDenied as exc:
        exit_from_exc(
            exc, message=f"No permission to access schema '{schema_full_name}'.", code=1
        )

    if name:
        rx = re.compile(name)
        tables = [t for t in tables if rx.search(t.full_name)]

    if not tables:
        out.warn("No tables found.")
        raise typer.Exit(0)

    full_names = [t.full_name for t in tables]
    out.header("Matched tables")
    out.tables_table(tables, title="Matched tables")

    selected = (
        full_names
        if all_
        else out.select_many("Select tables to change owner:", full_names)
    )
    if not selected:
        out.warn("No tables selected.")
        raise typer.Exit(0)

    out.header("Selected tables")
    out.tables_table(selected, title="Selected tables")
    out.info(f"New owner: {owner}")

    if dry_run:
        out.warn("DRY RUN: no changes will be made.")
        with out.status("Building owner change preview..."):
            results = set_tables_owner(adapter, selected, owner, dry_run=True)
        out.uc_owner_change_results_table(results, title="Owner change (dry-run)")
        raise typer.Exit(0)

    if not yes:
        if not out.confirm("Proceed with changing owner for the selected tables?"):
            out.warn("Cancelled.")
            raise typer.Exit(0)

    with out.status("Updating table owners..."):
        results = set_tables_owner(adapter, selected, owner, dry_run=False)
    out.uc_owner_change_results_table(results, title="Owner change results")

    failed = [r for r in results if not getattr(r, "ok", False)]
    if failed:
        out.error(f"Failed to change owner for {len(failed)} table(s).")
        raise typer.Exit(1)

    out.success(f"Owner changed for {len(results)} table(s).")


@uc_app.command("tables-delete")
def tables_delete(
    ctx: typer.Context,
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
):
    """Delete one or more Unity Catalog tables (owner -> delete)."""
    appctx: UCAppContext = ctx.obj
    adapter = appctx.adapter

    schema_full_name = schema or schema_arg
    if not schema_full_name:
        out.error(
            "Missing schema. Provide it as a positional argument or via --schema."
        )
        raise typer.Exit(2)

    catalog, schema_name = parse_schema_full_name(schema_full_name)
    try:
        with out.status("Loading tables..."):
            tables = adapter.list_tables(catalog=catalog, schema=schema_name)
    except NotFound as exc:
        exit_from_exc(
            exc, message=f"Schema '{catalog}.{schema_name}' does not exist.", code=1
        )
    except PermissionDenied as exc:
        exit_from_exc(
            exc,
            message=f"No permission to access schema '{catalog}.{schema_name}'.",
            code=1,
        )

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

    status_msg = "Planning table deletions..." if dry_run else "Deleting tables..."
    with out.status(status_msg):
        results = delete_tables(adapter, selected, dry_run=dry_run)

    out.uc_delete_results_table(results, title="Delete results")

    failed = [r for r in results if getattr(r, "error", None)]
    if failed:
        out.error(f"Failed to delete {len(failed)} table(s).")
        raise typer.Exit(1)

    out.success(f"Deleted {len(results)} table(s).")


@uc_app.command("schema-delete")
def schema_delete(
    ctx: typer.Context,
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
):
    """Delete schema after taking ownership and deleting tables within it."""
    appctx: UCAppContext = ctx.obj
    adapter = appctx.adapter

    if "." not in schema:
        out.error("Schema must be in the form catalog.schema")
        raise typer.Exit(2)

    try:
        with out.status("Building deletion plan..."):
            plan = delete_schema_with_tables(
                adapter,
                schema_full_name=schema,
                table_name_regex=name,
                force_schema_delete=force,
                dry_run=True,
            )
    except NotFound as exc:
        exit_from_exc(exc, message=f"Schema '{schema}' does not exist.", code=1)
    except PermissionDenied as exc:
        exit_from_exc(
            exc, message=f"No permission to access schema '{schema}'.", code=1
        )

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

    with out.status("Deleting tables and schema..."):
        delete_schema_with_tables(
            adapter,
            schema_full_name=schema,
            table_name_regex=name,
            force_schema_delete=force,
            dry_run=False,
        )

    out.success("Schema deletion completed.")


@uc_app.command("schemas-drop-empty")
def schemas_drop_empty(
    ctx: typer.Context,
    catalog: str = typer.Option(..., "--catalog", help="Catalog name"),
    name: str | None = typer.Option(
        None, "--name", help="Optional regex filter on schema full name"
    ),
    all_: bool = typer.Option(
        False, "--all", help="Drop all matched empty schemas without selection UI"
    ),
    force: bool = typer.Option(False, "--force", help="Force schema deletion"),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Show what would be deleted, but do nothing"
    ),
    yes: bool = typer.Option(False, "--yes", help="Skip confirmation prompt"),
):
    """Drop schemas that are currently empty (owner -> current user -> drop)."""
    appctx: UCAppContext = ctx.obj
    adapter = appctx.adapter

    try:
        with out.status("Scanning schemas for empties..."):
            empty = find_empty_schemas(adapter, catalog=catalog, name_regex=name)
    except NotFound as exc:
        exit_from_exc(exc, message=f"Catalog '{catalog}' does not exist.", code=1)
    except PermissionDenied as exc:
        exit_from_exc(
            exc, message=f"No permission to access catalog '{catalog}'.", code=1
        )

    if not empty:
        out.warn("No empty schemas found.")
        raise typer.Exit(0)

    out.header("Empty schemas")
    out.schemas_table(empty, title="Empty schemas")

    selected = (
        empty if all_ else out.select_many("Select empty schemas to drop:", empty)
    )
    if not selected:
        out.warn("No schemas selected.")
        raise typer.Exit(0)

    out.header("Selected schemas")
    out.schemas_table(selected, title="Selected schemas")

    if dry_run:
        out.warn("DRY RUN: no changes will be made.")
        with out.status("Planning schema drops..."):
            results = drop_empty_schemas(adapter, selected, force=force, dry_run=True)
        out.uc_schema_drop_results_table(results, title="Schema drop (dry-run)")
        raise typer.Exit(0)

    if not yes:
        if not out.confirm("Proceed with dropping the selected empty schemas?"):
            out.warn("Cancelled.")
            raise typer.Exit(0)

    with out.status("Dropping schemas..."):
        results = drop_empty_schemas(adapter, selected, force=force, dry_run=False)
    out.uc_schema_drop_results_table(results, title="Schema drop results")

    failed = [r for r in results if not getattr(r, "ok", False)]
    if failed:
        out.error(f"Failed to drop {len(failed)} schema(s).")
        raise typer.Exit(1)

    out.success(f"Dropped {len(results)} empty schema(s).")

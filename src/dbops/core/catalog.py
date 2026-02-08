from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable

from dbops.core.adapters.unitycatalog import UnityCatalogAdapter
from dbops.core.uc import UCTable


@dataclass(frozen=True)
class UCTableDeleteResult:
    """Result for a single UC table delete operation."""

    table: str
    owner_set: bool
    deleted: bool
    error: str | None = None


def parse_schema_full_name(schema_full_name: str) -> tuple[str, str]:
    """Split `catalog.schema` into (catalog, schema)."""
    parts = schema_full_name.strip().split(".")
    if len(parts) != 2:
        raise ValueError("Schema must be in the form `catalog.schema`.")
    catalog, schema = parts
    if not catalog or not schema:
        raise ValueError("Schema must be in the form `catalog.schema`.")
    return catalog, schema


def filter_tables(tables: list[UCTable], name_regex: str | None) -> list[UCTable]:
    """Filter tables by regex on full_name (or keep all if regex is None)."""
    if not name_regex:
        return tables
    rx = re.compile(name_regex)
    return [t for t in tables if rx.search(t.full_name)]


def delete_tables(
    adapter: UnityCatalogAdapter,
    table_full_names: list[str],
    *,
    dry_run: bool = False,
) -> list[UCTableDeleteResult]:
    """
    For each table:
      1) set owner to current user
      2) delete table
    """
    results: list[UCTableDeleteResult] = []
    owner = adapter.current_username() if not dry_run else None

    for full_name in table_full_names:
        if dry_run:
            results.append(
                UCTableDeleteResult(table=full_name, owner_set=False, deleted=False)
            )
            continue

        try:
            if owner is None:
                raise RuntimeError("Owner resolution failed for table delete.")
            adapter.set_table_owner(full_name, owner=owner)
            adapter.delete_table(full_name)
            results.append(
                UCTableDeleteResult(table=full_name, owner_set=True, deleted=True)
            )
        except Exception as e:  # keep CLI resilient; surface per-table errors
            results.append(
                UCTableDeleteResult(
                    table=full_name, owner_set=False, deleted=False, error=str(e)
                )
            )

    return results


def delete_schema_with_tables(
    adapter: UnityCatalogAdapter,
    schema_full_name: str,
    *,
    table_name_regex: str | None = None,
    force_schema_delete: bool = False,
    dry_run: bool = False,
) -> dict[str, object]:
    """
    Delete schema by:
      1) set schema owner to current user
      2) list tables in schema (optionally regex-filtered)
      3) set table owner -> delete each table
      4) delete schema
    """
    owner = adapter.current_username()
    catalog, schema = parse_schema_full_name(schema_full_name)

    tables = adapter.list_tables(catalog=catalog, schema=schema)
    tables = filter_tables(tables, table_name_regex)
    table_names = [t.full_name for t in tables]

    if dry_run:
        return {
            "schema": schema_full_name,
            "owner": owner,
            "tables": table_names,
            "schema_deleted": False,
        }

    adapter.set_schema_owner(schema_full_name, owner=owner)
    table_results = delete_tables(adapter, table_names, dry_run=False)
    adapter.delete_schema(schema_full_name, force=force_schema_delete)

    return {
        "schema": schema_full_name,
        "owner": owner,
        "tables": table_names,
        "table_results": table_results,
        "schema_deleted": True,
    }


@dataclass(frozen=True)
class UCOwnerChangeResult:
    """Result of an ownership change action for a UC object (table/schema)."""

    full_name: str
    new_owner: str
    ok: bool
    error: str | None = None


@dataclass(frozen=True)
class UCSchemaDropResult:
    """Result of dropping a UC schema."""

    schema_full_name: str
    ok: bool
    error: str | None = None


def set_tables_owner(
    adapter,
    table_full_names: Iterable[str],
    owner: str,
    *,
    dry_run: bool,
) -> list[UCOwnerChangeResult]:
    """Set the owner of one or more Unity Catalog tables."""
    results: list[UCOwnerChangeResult] = []
    for full_name in table_full_names:
        if dry_run:
            results.append(
                UCOwnerChangeResult(full_name=full_name, new_owner=owner, ok=True)
            )
            continue
        try:
            adapter.set_table_owner(full_name=full_name, owner=owner)
            results.append(
                UCOwnerChangeResult(full_name=full_name, new_owner=owner, ok=True)
            )
        except Exception as e:  # noqa: BLE001
            results.append(
                UCOwnerChangeResult(
                    full_name=full_name, new_owner=owner, ok=False, error=str(e)
                )
            )
    return results


def find_empty_schemas(
    adapter,
    catalog: str,
    *,
    name_regex: str | None = None,
) -> list[str]:
    """Return schema full names (catalog.schema) that currently contain zero tables."""
    rx = re.compile(name_regex) if name_regex else None
    empty: list[str] = []

    schemas = adapter.list_schemas(catalog=catalog)
    for s in schemas:
        schema_name = getattr(s, "name", None)
        schema_full_name = getattr(s, "full_name", None)
        if not schema_name or not schema_full_name:
            continue
        if rx and not rx.search(schema_full_name):
            continue

        tables = adapter.list_tables(catalog=catalog, schema=schema_name)
        if len(tables) == 0:
            empty.append(schema_full_name)

    return empty


def drop_empty_schemas(
    adapter,
    schema_full_names: Iterable[str],
    *,
    force: bool = False,
    dry_run: bool,
) -> list[UCSchemaDropResult]:
    """Drop schemas that are already empty (owner will be set to current user first)."""
    me = adapter.current_username() if not dry_run else None
    results: list[UCSchemaDropResult] = []

    for schema_full_name in schema_full_names:
        if dry_run:
            results.append(
                UCSchemaDropResult(schema_full_name=schema_full_name, ok=True)
            )
            continue
        try:
            if me is None:
                raise RuntimeError("Owner resolution failed for schema drop.")
            adapter.set_schema_owner(schema_full_name=schema_full_name, owner=me)
            adapter.delete_schema(schema_full_name=schema_full_name, force=force)
            results.append(
                UCSchemaDropResult(schema_full_name=schema_full_name, ok=True)
            )
        except Exception as e:  # noqa: BLE001
            results.append(
                UCSchemaDropResult(
                    schema_full_name=schema_full_name, ok=False, error=str(e)
                )
            )

    return results

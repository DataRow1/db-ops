from __future__ import annotations

import re
from dataclasses import dataclass

from db_ops.core.adapters.unitycatalog import UCTable, UnityCatalogAdapter


@dataclass(frozen=True)
class UCTableDeleteResult:
    """Result for a single UC table delete operation."""

    table: str
    owner_set: bool
    deleted: bool
    error: str | None = None


def parse_schema_full_name(schema_full_name: str) -> tuple[str, str]:
    """Split `catalog.schema` into (catalog, schema)."""
    parts = schema_full_name.split(".")
    if len(parts) != 2:
        raise ValueError("Schema must be in the form `catalog.schema`.")
    return parts[0], parts[1]


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
    owner = adapter.current_username()
    results: list[UCTableDeleteResult] = []

    for full_name in table_full_names:
        if dry_run:
            results.append(
                UCTableDeleteResult(table=full_name, owner_set=False, deleted=False)
            )
            continue

        try:
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

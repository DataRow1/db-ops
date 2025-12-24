"""Core domain models for Unity Catalog.

These models represent Unity Catalog entities in a simple, immutable form.
They are intentionally free of Databricks SDK types and UI/CLI concerns.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class UCTable:
    """Lightweight representation of a Unity Catalog table."""

    full_name: str
    owner: str | None = None
    table_type: str | None = None


@dataclass(frozen=True)
class UCCatalog:
    """Lightweight representation of a Unity Catalog catalog."""

    name: str
    owner: str | None = None


@dataclass(frozen=True)
class UCSchema:
    """Lightweight representation of a Unity Catalog schema."""

    full_name: str
    name: str
    catalog_name: str
    owner: str | None = None

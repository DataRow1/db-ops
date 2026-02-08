"""Output formatting utilities for the CLI."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

import questionary
from rich.console import Console
from rich.table import Table
from rich.theme import Theme

from dbops.cli.common.tui_style import (
    QUESTIONARY_STYLE_CONFIRM,
    QUESTIONARY_STYLE_SELECT,
)

_THEME = Theme(
    {
        "ok": "bold green",
        "warn": "yellow",
        "err": "bold red",
        "title": "bold cyan",
        "meta": "dim",
    }
)

console = Console(theme=_THEME)


@dataclass(frozen=True)
class Out:
    """Output formatter for CLI messages and tables."""

    def _q_try(self, fn, *args, **kwargs):
        """Call questionary prompts and drop unsupported kwargs on older versions."""
        try:
            return fn(*args, **kwargs)
        except TypeError:
            for k in ("pointer", "checked_icon", "unchecked_icon", "auto_enter"):
                kwargs.pop(k, None)
            return fn(*args, **kwargs)

    def _q(self, message: str) -> str:
        """Prefix Questionary prompts to be BRICK-OPS consistent."""
        return f"[BRICK-OPS] {message}"

    def info(self, msg: str) -> None:
        """Print an info message."""
        console.print(f"[title]›[/] {msg}")

    @contextmanager
    def status(self, msg: str):
        """Show a transient status spinner while work is in progress."""
        with console.status(msg, spinner="dots"):
            yield

    def success(self, msg: str) -> None:
        """Print a success message."""
        console.print(f"[ok]✓[/] {msg}")

    def warn(self, msg: str) -> None:
        """Print a warning message."""
        console.print(f"[warn]⚠[/] {msg}")

    def error(self, msg: str) -> None:
        """Print an error message."""
        console.print(f"[err]✗[/] {msg}")

    def header(self, title: str) -> None:
        """Print a header message."""
        console.print(f"[title]{title}[/]")

    def print(self, msg: str) -> None:
        """Print a raw Rich-formatted message to the console."""
        console.print(msg)

    def kv(self, items: Mapping[str, Any]) -> None:
        """Print key-value pairs."""
        for k, v in items.items():
            console.print(f"[meta]{k}[/]: {v}")

    def select_many(self, message: str, choices: list[str]) -> list[str]:
        """
        Prompt the user to select multiple items from a list.

        Uses questionary checkbox UI to keep BRICK-OPS UX consistent.
        Returns a list of selected values.
        """
        if not choices:
            return []

        prompt = self._q_try(
            questionary.checkbox,
            self._q(message),
            choices=choices,
            style=QUESTIONARY_STYLE_SELECT,
            qmark="✦",
            instruction="Use ↑/↓, space, a (all), i (invert), enter",
            pointer="❯",
            checked_icon="▣",
            unchecked_icon="▢",
        )
        picked = prompt.ask()
        return list(picked or [])

    def select_one(self, message: str, choices: list[str]) -> str | None:
        """
        Prompt the user to select a single item from a list (radio list).

        Returns:
            The selected value, or None if cancelled.
        """
        if not choices:
            return None

        prompt = self._q_try(
            questionary.select,
            self._q(message),
            choices=choices,
            style=QUESTIONARY_STYLE_SELECT,
            qmark="✦",
            instruction="Use ↑/↓ then Enter",
            pointer="❯",
        )
        return prompt.ask()

    def confirm(self, message: str, *, default: bool = False) -> bool:
        """
        Ask the user for confirmation using a standardized Questionary prompt.

        This wrapper ensures consistent styling and behavior across all
        interactive confirmation prompts in the CLI.

        Args:
            message: Confirmation question shown to the user.
            default: Default answer if the user just presses enter.

        Returns:
            True if the user confirms, False otherwise.
        """
        # Print instruction on its own line (Questionary renders `instruction=...` inline,
        # which looks odd next to the final echoed answer).
        console.print("[meta]Use y/n then Enter[/]")

        prompt = self._q_try(
            questionary.confirm,
            self._q(message),
            default=default,
            style=QUESTIONARY_STYLE_CONFIRM,
            qmark="✦",
            auto_enter=False,
            pointer="❯",  # dropped automatically on older Questionary versions
        )
        return bool(prompt.ask())

    def jobs_table(self, jobs: Iterable[Any], title: str = "Jobs") -> None:
        """
        Expects objects with .id .name .tags (like dbops.core.models.Job)
        """
        t = Table(title=title, show_lines=False)
        t.add_column("Job ID", style="ok", no_wrap=True)
        t.add_column("Name")
        t.add_column("Tags", style="meta")

        for j in jobs:
            tags = ", ".join(
                f"{k}={v}" for k, v in (getattr(j, "tags", None) or {}).items()
            )
            t.add_row(str(j.id), j.name, tags)

        console.print(t)

    def runs_table(self, runs: Iterable[Any], title: str = "Runs") -> None:
        """
        Expects objects with .job_id and .run_id
        (e.g. dbops.core.models.JobRun)
        """
        t = Table(title=title, show_lines=False)
        t.add_column("Job ID", style="ok", no_wrap=True)
        t.add_column("Run ID", style="ok", no_wrap=True)

        for r in runs:
            t.add_row(str(r.job_id), str(r.run_id))

        console.print(t)

    def run_status_table(
        self, results: Iterable[tuple[Any, Any]], title: str = "Run status"
    ) -> None:
        """
        Expects tuples of (JobRun, RunStatus)
        """
        t = Table(title=title, show_lines=False)
        t.add_column("Job ID", style="ok", no_wrap=True)
        t.add_column("Run ID", style="ok", no_wrap=True)
        t.add_column("Status")

        for run, status in results:
            status_value = status.value if hasattr(status, "value") else str(status)

            style = "ok" if status_value == "SUCCESS" else "err"

            t.add_row(
                str(run.job_id),
                str(run.run_id),
                f"[{style}]{status_value}[/{style}]",
            )

        console.print(t)

    def tables_table(self, tables: Iterable[Any], title: str = "Tables") -> None:
        """
        Render a table preview of Unity Catalog tables.

        Accepts either:
          - strings with fully qualified table names, or
          - objects with `.full_name`, optional `.owner`, optional `.table_type`.
        """
        t = Table(title=title, show_lines=False)
        t.add_column("Full name", style="ok")
        t.add_column("Type", style="meta")
        t.add_column("Owner", style="meta")

        for item in tables:
            if isinstance(item, str):
                full_name = item
                table_type = ""
                owner = ""
            else:
                full_name = getattr(item, "full_name", "") or ""
                table_type = str(getattr(item, "table_type", "") or "")
                owner = str(getattr(item, "owner", "") or "")

            t.add_row(full_name, table_type, owner)

        console.print(t)

    def uc_delete_results_table(
        self, results: Iterable[Any], title: str = "Delete results"
    ) -> None:
        """
        Render a results table for Unity Catalog table deletions.

        Expects objects with:
          - `.table` (full name)
          - `.owner_set` (bool)
          - `.deleted` (bool)
          - optional `.error` (str | None)
        """
        t = Table(title=title, show_lines=False)
        t.add_column("Table", style="ok")
        t.add_column("Owner set")
        t.add_column("Deleted")
        t.add_column("Error", style="err")

        for r in results:
            owner_set = "yes" if getattr(r, "owner_set", False) else "no"
            deleted = "yes" if getattr(r, "deleted", False) else "no"
            err = str(getattr(r, "error", "") or "")
            t.add_row(str(getattr(r, "table", "")), owner_set, deleted, err)

        console.print(t)

    def uc_owner_change_results_table(
        self, results, title: str = "Owner change results"
    ) -> None:
        """Render results of UC ownership changes (success/fail per object)."""
        t = Table(title=title, show_lines=False)
        t.add_column("Object", style="ok")
        t.add_column("New owner", style="meta")
        t.add_column("Result")

        for r in results:
            name = str(getattr(r, "full_name", "") or "")
            owner = str(getattr(r, "new_owner", "") or "")
            ok = bool(getattr(r, "ok", False))
            err = getattr(r, "error", None)
            t.add_row(name, owner, "[ok]OK[/]" if ok else f"[err]FAIL[/] {err}")

        console.print(t)

    def uc_schema_drop_results_table(
        self, results, title: str = "Schema drop results"
    ) -> None:
        """Render results of dropping UC schemas (success/fail per schema)."""
        t = Table(title=title, show_lines=False)
        t.add_column("Schema", style="ok")
        t.add_column("Result")

        for r in results:
            name = str(getattr(r, "schema_full_name", "") or "")
            ok = bool(getattr(r, "ok", False))
            err = getattr(r, "error", None)
            t.add_row(name, "[ok]OK[/]" if ok else f"[err]FAIL[/] {err}")

        console.print(t)

    def schemas_table(self, schemas: list[str], title: str = "Schemas") -> None:
        """Render a table of Unity Catalog schema full names (catalog.schema)."""
        t = Table(title=title, show_lines=False)
        t.add_column("Schema", style="ok")

        for s in schemas:
            t.add_row(str(s))

        console.print(t)


out = Out()

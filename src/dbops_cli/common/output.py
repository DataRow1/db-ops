"""Output formatting utilities for the CLI."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping

import questionary
from rich.console import Console
from rich.table import Table
from rich.theme import Theme

from dbops_cli.common.tui_style import (
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

    def info(self, msg: str) -> None:
        """Print an info message."""
        console.print(f"[title]›[/] {msg}")

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

        picked = questionary.checkbox(
            message,
            choices=choices,
            style=QUESTIONARY_STYLE_SELECT,
            qmark="✦",
            pointer="❯",
            instruction="Use ↑/↓, space, enter",
            checked_icon="▣",  # selected
            unchecked_icon="▢",  # not selected
        ).ask()

        return list(picked or [])

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

        return bool(
            questionary.confirm(
                message,
                default=default,
                style=QUESTIONARY_STYLE_CONFIRM,
                qmark="✦",
                pointer="❯",
                instruction="Use ←/→ then Enter",
            ).ask()
        )

    def jobs_table(self, jobs: Iterable[Any], title: str = "Jobs") -> None:
        """
        Expects objects with .id .name .tags (like db_ops.core.models.Job)
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
        (e.g. db_ops.core.models.JobRun)
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


out = Out()

"""Exit handling utilities for the CLI."""

from typing import NoReturn

import typer

from dbops.cli.common.output import out


def ok_exit(msg: str | None = None) -> "None":
    """Exit successfully with an optional informational message."""
    if msg:
        out.info(msg)
    raise typer.Exit(0)


def die(msg: str, code: int = 1) -> "None":
    """Exit with an error message and optional exit code."""
    out.error(msg)
    raise typer.Exit(code)


def warn_exit(msg: str, code: int = 0) -> "None":
    """Exit with a warning message and optional exit code."""
    out.warn(msg)
    raise typer.Exit(code)


def exit_from_exc(exc: Exception, *, message: str, code: int = 1) -> NoReturn:
    """
    Helper function to print an error message and exit with a given code.

    Exists to satisfy pylint W0707 and to standardize error exits.
    """
    out.error(message)
    raise typer.Exit(code) from exc

"""Authentication helpers for Databricks.

This module centralizes creation of a Databricks WorkspaceClient and applies
small but important normalization rules (such as sanitizing the host URL)
to avoid subtle SDK and API issues.
"""

import re

from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config


class AuthError(RuntimeError):
    """Raised when Databricks authentication fails."""


def _format_auth_error(message: str, profile: str | None) -> str:
    """Return a user-friendly auth error message."""
    login_match = re.search(r"databricks auth login ([^\\s]+)", message)
    host = login_match.group(1) if login_match else None
    if host:
        cmd = "databricks auth login"
        if profile:
            cmd = f"{cmd} --profile {profile}"
        return (
            "Databricks authentication failed. Your refresh token is invalid.\n"
            f"Re-authenticate with:\n  $ {cmd}"
        )
    return f"Databricks authentication failed: {message}"


def _sanitize_host(host: str | None) -> str | None:
    """
    Normalize a Databricks host URL.

    - Removes query strings (e.g. '?o=123456789')
    - Removes trailing slashes

    This ensures the host value is compatible with the Databricks SDK
    and avoids malformed API URLs.
    """
    if not host:
        return host
    # strip querystring zoals ?o=....
    host = host.split("?", 1)[0]
    # strip trailing slash
    return host.rstrip("/")


def get_client(profile: str | None = None) -> WorkspaceClient:
    """
    Create and return a configured Databricks WorkspaceClient.

    If a profile is provided, it is resolved using the Databricks unified
    authentication configuration (~/.databrickscfg or environment variables).

    The host URL is sanitized to remove query strings and trailing slashes
    before constructing the client.
    """
    try:
        cfg = Config(profile=profile) if profile else Config()
    except ValueError as exc:
        raise AuthError(_format_auth_error(str(exc), profile)) from exc
    cfg.host = _sanitize_host(cfg.host)
    return WorkspaceClient(config=cfg)

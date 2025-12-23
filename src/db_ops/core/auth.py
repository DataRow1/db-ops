"""Authentication helpers for Databricks.

This module centralizes creation of a Databricks WorkspaceClient and applies
small but important normalization rules (such as sanitizing the host URL)
to avoid subtle SDK and API issues.
"""

from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config


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
    cfg = Config(profile=profile) if profile else Config()
    cfg.host = _sanitize_host(cfg.host)
    return WorkspaceClient(config=cfg)

"""Application context management for the CLI."""

from dataclasses import dataclass

from databricks.sdk import WorkspaceClient

from dbops.cli.common.exits import die
from dbops.core.adapters.databricksjobs import DatabricksJobsAdapter
from dbops.core.adapters.unitycatalog import UnityCatalogAdapter
from dbops.core.auth import AuthError, get_client


@dataclass
class JobsAppContext:
    """Application context holding Databricks client and jobs adapter configuration."""

    profile: str | None
    client: WorkspaceClient
    adapter: DatabricksJobsAdapter


@dataclass
class UCAppContext:
    """Application context holding Databricks client and Unity Catalog adapter."""

    profile: str | None
    client: WorkspaceClient
    adapter: UnityCatalogAdapter


def build_jobs_context(
    profile: str | None, *, refresh_jobs: bool = False
) -> JobsAppContext:
    """Build and return the application context with Databricks client and adapter.

    Args:
        profile: Optional Databricks profile name to use for authentication.

    Returns:
        JobsAppContext: Application context with configured client and adapter.
    """
    try:
        client = get_client(profile)
    except AuthError as exc:
        die(str(exc), code=1)
    adapter = DatabricksJobsAdapter(client, profile=profile, force_refresh=refresh_jobs)
    return JobsAppContext(profile=profile, client=client, adapter=adapter)


def build_uc_context(profile: str | None) -> UCAppContext:
    """Build and return the application context for Unity Catalog commands."""
    try:
        client = get_client(profile)
    except AuthError as exc:
        die(str(exc), code=1)
    adapter = UnityCatalogAdapter(client)
    return UCAppContext(profile=profile, client=client, adapter=adapter)

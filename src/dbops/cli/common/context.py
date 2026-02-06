"""Application context management for the CLI."""

from dataclasses import dataclass

from databricks.sdk import WorkspaceClient

from dbops.cli.common.exits import die
from dbops.core.adapters.databricksjobs import DatabricksJobsAdapter
from dbops.core.auth import AuthError, get_client


@dataclass
class AppContext:
    """Application context holding Databricks client and job adapter configuration."""

    profile: str | None
    client: WorkspaceClient
    adapter: DatabricksJobsAdapter


def build_context(profile: str | None, *, refresh_jobs: bool = False) -> AppContext:
    """Build and return the application context with Databricks client and adapter.

    Args:
        profile: Optional Databricks profile name to use for authentication.

    Returns:
        AppContext: Application context with configured client and adapter.
    """
    try:
        client = get_client(profile)
    except AuthError as exc:
        die(str(exc), code=1)
    adapter = DatabricksJobsAdapter(client, profile=profile, force_refresh=refresh_jobs)
    return AppContext(profile=profile, client=client, adapter=adapter)

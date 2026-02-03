"""Application context management for the CLI."""

from dataclasses import dataclass

from databricks.sdk import WorkspaceClient

from dbops.core.adapters.databricksjobs import DatabricksJobsAdapter
from dbops.core.auth import get_client


@dataclass
class AppContext:
    """Application context holding Databricks client and job adapter configuration."""

    profile: str | None
    client: WorkspaceClient
    adapter: DatabricksJobsAdapter


def build_context(profile: str | None) -> AppContext:
    """Build and return the application context with Databricks client and adapter.

    Args:
        profile: Optional Databricks profile name to use for authentication.

    Returns:
        AppContext: Application context with configured client and adapter.
    """
    client = get_client(profile)
    adapter = DatabricksJobsAdapter(client)
    return AppContext(profile=profile, client=client, adapter=adapter)

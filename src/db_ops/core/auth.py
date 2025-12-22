from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config


def _sanitize_host(host: str | None) -> str | None:
    if not host:
        return host
    # strip querystring zoals ?o=....
    host = host.split("?", 1)[0]
    # strip trailing slash
    return host.rstrip("/")


def get_client(profile: str | None = None) -> WorkspaceClient:
    cfg = Config(profile=profile) if profile else Config()
    cfg.host = _sanitize_host(cfg.host)
    return WorkspaceClient(config=cfg)
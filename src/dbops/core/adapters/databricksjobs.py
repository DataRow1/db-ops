from __future__ import annotations

import json
import os
import re
import time
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunResultState

from dbops.core.jobs import Job, JobRun, RunStatus


class DatabricksJobsAdapter:
    """Adapter around Databricks SDK Jobs APIs."""

    _CACHE_TTL_ENV = "DBOPS_JOBS_CACHE_TTL"
    _CACHE_DISABLE_ENV = "DBOPS_JOBS_CACHE_DISABLE"
    _CACHE_DIR_ENV = "DBOPS_CACHE_DIR"
    _DEFAULT_CACHE_TTL_SECONDS = 300

    def __init__(self, client: WorkspaceClient, profile: str | None = None):
        """Create a jobs adapter for a Databricks workspace."""
        self.client = client
        self.profile = profile or "default"
        self._cache_path = self._build_cache_path()

    def _build_cache_path(self) -> Path:
        """Return the cache file path for this workspace/profile."""
        cache_root = os.getenv(self._CACHE_DIR_ENV)
        if cache_root:
            base = Path(cache_root)
        else:
            xdg = os.getenv("XDG_CACHE_HOME")
            base = Path(xdg) if xdg else Path.home() / ".cache"
        cache_dir = base / "dbops"
        host = getattr(getattr(self.client, "config", None), "host", None) or "unknown"
        key = f"{self.profile}@{host}"
        safe_key = re.sub(r"[^A-Za-z0-9_.-]+", "_", key)
        return cache_dir / f"jobs_{safe_key}.json"

    def _cache_ttl_seconds(self) -> int:
        """Return cache TTL in seconds, honoring env override."""
        raw = os.getenv(self._CACHE_TTL_ENV)
        if raw is None:
            return self._DEFAULT_CACHE_TTL_SECONDS
        try:
            return max(int(raw), 0)
        except ValueError:
            return self._DEFAULT_CACHE_TTL_SECONDS

    def _cache_enabled(self) -> bool:
        """Return True if caching is enabled."""
        disabled = os.getenv(self._CACHE_DISABLE_ENV, "").strip().lower()
        if disabled in {"1", "true", "yes"}:
            return False
        return self._cache_ttl_seconds() > 0

    def _load_cached_jobs(self) -> list[Job] | None:
        """Load cached jobs if the cache is fresh."""
        if not self._cache_enabled():
            return None
        path = self._cache_path
        if not path.exists():
            return None
        try:
            payload = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError):
            return None
        timestamp = payload.get("timestamp")
        if not isinstance(timestamp, (int, float)):
            return None
        if time.time() - float(timestamp) > self._cache_ttl_seconds():
            return None
        jobs = []
        for item in payload.get("jobs", []):
            try:
                jobs.append(
                    Job(
                        id=int(item["id"]),
                        name=str(item["name"]),
                        tags=item.get("tags") or {},
                    )
                )
            except (KeyError, TypeError, ValueError):
                continue
        return jobs

    def _store_cached_jobs(self, jobs: list[Job]) -> None:
        """Persist jobs to the cache on disk."""
        if not self._cache_enabled():
            return
        path = self._cache_path
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "timestamp": time.time(),
            "jobs": [
                {"id": job.id, "name": job.name, "tags": dict(job.tags or {})}
                for job in jobs
            ],
        }
        path.write_text(json.dumps(payload))

    def find_all_jobs(self) -> list[Job]:
        """Return all jobs in the workspace (cached when enabled)."""
        cached = self._load_cached_jobs()
        if cached is not None:
            return cached

        jobs: list[Job] = []

        for j in self.client.jobs.list():
            if not j.settings or not j.settings.name:
                continue

            jobs.append(
                Job(
                    id=j.job_id,
                    name=j.settings.name,
                    tags=j.settings.tags or {},
                )
            )

        self._store_cached_jobs(jobs)
        return jobs

    def start_job(self, job_id: int) -> JobRun:
        """Start a Databricks job and return its run handle."""
        run = self.client.jobs.run_now(job_id=job_id)
        return JobRun(run_id=run.run_id, job_id=job_id)

    def get_run_status(self, run_id: int) -> RunStatus:
        """Return the current status for a Databricks job run."""
        run = self.client.jobs.get_run(run_id)
        state = run.state

        if not state:
            return RunStatus.UNKNOWN

        if state.result_state == RunResultState.SUCCESS:
            return RunStatus.SUCCESS
        if state.result_state == RunResultState.FAILED:
            return RunStatus.FAILED
        if state.result_state == RunResultState.CANCELED:
            return RunStatus.CANCELED

        if state.life_cycle_state:
            return RunStatus.RUNNING

        return RunStatus.UNKNOWN

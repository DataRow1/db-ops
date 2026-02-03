#!/usr/bin/env python3
"""Validate Conventional Commit messages in a commit range.

Runs in CI to ensure PR commit subjects follow Conventional Commits.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
from pathlib import Path

CONVENTIONAL_RE = re.compile(
    r"^(feat|fix|docs|style|refactor|perf|test|build|ci|chore|revert)(\([\w\-./]+\))?(!)?: .+"
)


def _run(*args: str) -> str:
    return subprocess.check_output(args, text=True).strip()


def _commit_range() -> str:
    event_name = os.getenv("GITHUB_EVENT_NAME", "")
    if event_name == "pull_request":
        base_ref = os.getenv("GITHUB_BASE_REF", "")
        head = os.getenv("GITHUB_SHA", "HEAD")
        if base_ref:
            return f"origin/{base_ref}..{head}"

    event_path = os.getenv("GITHUB_EVENT_PATH")
    if event_path and Path(event_path).exists():
        with open(event_path, "r", encoding="utf-8") as handle:
            event = json.load(handle)
        before = event.get("before")
        after = event.get("after")
        if before and after:
            return f"{before}..{after}"

    return "HEAD~20..HEAD"


def _is_valid(subject: str) -> bool:
    if subject.startswith("Merge "):
        return True
    if subject.startswith("Revert "):
        return True
    return bool(CONVENTIONAL_RE.match(subject))


def main() -> int:
    commit_range = _commit_range()
    try:
        subjects = _run("git", "log", "--format=%s", commit_range).splitlines()
    except subprocess.CalledProcessError as exc:
        print(f"Failed to read commits for range: {commit_range}")
        print(exc)
        return 2

    invalid = [s for s in subjects if not _is_valid(s)]
    if not invalid:
        print("Conventional Commit check passed.")
        return 0

    print("Conventional Commit check failed. Invalid commit subjects:")
    for subject in invalid:
        print(f"- {subject}")
    print("\nExpected format: type(scope?): subject")
    print("Example: feat(cli): add jobs list")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

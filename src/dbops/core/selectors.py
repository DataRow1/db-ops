"""Job selector abstractions and implementations.

This module defines the selector system used to determine whether a
Databricks job matches a given set of criteria. Selectors encapsulate
matching logic and can be composed using logical operators (AND / OR)
to express complex selection rules.

Selectors are pure, side-effect-free objects and are intended to be
reusable across different frontends such as CLI commands, automation
scripts, and tests.
"""

from __future__ import annotations

import re
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dbops.core.jobs import Job


class JobSelector(ABC):
    """
    Abstract base class for all job selectors.

    A JobSelector encapsulates a single piece of matching logic that
    determines whether a given Job satisfies a specific criterion.
    """

    @abstractmethod
    def matches(self, job: Job) -> bool:
        """
        Determine whether the given job matches this selector.

        Args:
            job: Job instance to evaluate.

        Returns:
            True if the job matches the selector criteria, False otherwise.
        """
        ...


class NameRegexSelector(JobSelector):
    """
    Selector that matches jobs based on a regular expression applied
    to the job name.
    """

    def __init__(self, pattern: str):
        """
        Create a name-based regex selector.

        Args:
            pattern: Regular expression pattern used to match job names.
        """
        try:
            self.regex = re.compile(pattern)
        except re.error as exc:
            raise ValueError(f"Invalid regex expression: {exc}") from exc

    def matches(self, job: Job) -> bool:
        """
        Check whether the job name matches the configured regex pattern.
        """
        return bool(self.regex.search(job.name))


class TagSelector(JobSelector):
    """
    Selector that matches jobs based on a specific tag key-value pair.
    """

    def __init__(self, key: str, value: str):
        """
        Create a tag-based selector.

        Args:
            key: Tag key to match.
            value: Expected tag value.
        """
        self.key = key
        self.value = value

    def matches(self, job: Job) -> bool:
        """
        Check whether the job contains the specified tag with the expected value.
        """
        if not job.tags:
            return False
        return job.tags.get(self.key) == self.value


class AndSelector(JobSelector):
    """
    Composite selector that matches a job only if all child selectors match.
    """

    def __init__(self, selectors: list[JobSelector]):
        """
        Create a logical AND selector.

        Args:
            selectors: List of selectors that must all match.
        """
        self.selectors = selectors

    def matches(self, job: Job) -> bool:
        """
        Check whether all child selectors match the job.
        """
        return all(s.matches(job) for s in self.selectors)


class OrSelector(JobSelector):
    """
    Composite selector that matches a job if any child selector matches.
    """

    def __init__(self, selectors: list[JobSelector]):
        """
        Create a logical OR selector.

        Args:
            selectors: List of selectors where at least one must match.
        """
        self.selectors = selectors

    def matches(self, job: Job) -> bool:
        """
        Check whether any child selector matches the job.
        """
        return any(s.matches(job) for s in self.selectors)

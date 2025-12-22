import re
from abc import ABC, abstractmethod
from db_ops.core.models import Job


class JobSelector(ABC):
    @abstractmethod
    def matches(self, job: Job) -> bool: ...


class NameRegexSelector(JobSelector):
    def __init__(self, pattern: str):
        self.regex = re.compile(pattern)

    def matches(self, job: Job) -> bool:
        return bool(self.regex.search(job.name))


class TagSelector(JobSelector):
    def __init__(self, key: str, value: str):
        self.key = key
        self.value = value

    def matches(self, job: Job) -> bool:
        return job.tags.get(self.key) == self.value


class AndSelector(JobSelector):
    def __init__(self, selectors: list[JobSelector]):
        self.selectors = selectors

    def matches(self, job: Job) -> bool:
        return all(s.matches(job) for s in self.selectors)


class OrSelector(JobSelector):
    def __init__(self, selectors: list[JobSelector]):
        self.selectors = selectors

    def matches(self, job: Job) -> bool:
        return any(s.matches(job) for s in self.selectors)
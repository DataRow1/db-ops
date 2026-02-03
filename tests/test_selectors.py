from dbops.core.jobs import Job
from dbops.core.selectors import AndSelector, NameRegexSelector, OrSelector, TagSelector


def test_name_regex_selector_matches():
    job = Job(id=1, name="daily-etl", tags={"env": "prod"})
    selector = NameRegexSelector("daily")

    assert selector.matches(job) is True


def test_name_regex_selector_no_match():
    job = Job(id=2, name="weekly-etl", tags={"env": "prod"})
    selector = NameRegexSelector("daily")

    assert selector.matches(job) is False


def test_tag_selector_handles_missing_tags():
    job = Job(id=3, name="etl", tags=None)
    selector = TagSelector("env", "prod")

    assert selector.matches(job) is False


def test_and_or_selectors():
    job = Job(id=4, name="daily-etl", tags={"env": "prod"})

    name_sel = NameRegexSelector("daily")
    tag_sel = TagSelector("env", "prod")

    assert AndSelector([name_sel, tag_sel]).matches(job) is True
    assert OrSelector([name_sel, TagSelector("env", "dev")]).matches(job) is True

import pytest

from dbops.cli.common.selector_builder import build_selector
from dbops.core.selectors import AndSelector, NameRegexSelector, OrSelector, TagSelector


def test_build_selector_name_only():
    selector = build_selector(name="daily", tags=[], use_or=False)

    assert isinstance(selector, NameRegexSelector)


def test_build_selector_tag_only():
    selector = build_selector(name=None, tags=["env=prod"], use_or=False)

    assert isinstance(selector, TagSelector)


def test_build_selector_combined_and_or():
    and_selector = build_selector(name="daily", tags=["env=prod"], use_or=False)
    or_selector = build_selector(name="daily", tags=["env=prod"], use_or=True)

    assert isinstance(and_selector, AndSelector)
    assert isinstance(or_selector, OrSelector)


def test_build_selector_invalid_tag():
    with pytest.raises(ValueError):
        build_selector(name=None, tags=["broken"], use_or=False)

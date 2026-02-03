"""Selector construction utilities.

This module provides a small factory function that translates user intent
(such as CLI arguments or API inputs) into concrete JobSelector instances.
It centralizes validation and composition logic for selectors, ensuring that
the rest of the application can work with a single, well-defined selector
abstraction.
"""

from typing import Iterable

from dbops.core.selectors import (
    AndSelector,
    JobSelector,
    NameRegexSelector,
    OrSelector,
    TagSelector,
)


def build_selector(
    *,
    name: str | None,
    tags: Iterable[str],
    use_or: bool,
) -> JobSelector:
    """
    Build a composite JobSelector from user-provided criteria.

    This function converts optional name and tag filters into one or more
    concrete JobSelector instances and combines them using either logical
    AND or OR semantics.

    Validation is performed to ensure that:
    - At least one selector is provided
    - Tag selectors follow the expected `key=value` format

    Args:
        name: Optional regular expression used to match job names.
        tags: Iterable of tag selector strings in the form `key=value`.
        use_or: If True, combine multiple selectors using logical OR.
                If False, combine them using logical AND.

    Returns:
        A JobSelector instance representing the composed selection logic.

    Raises:
        ValueError: If no selectors are provided or if a tag selector
                    does not follow the `key=value` format.
    """
    selectors: list[JobSelector] = []

    if name:
        selectors.append(NameRegexSelector(name))

    for tag in tags:
        if "=" not in tag:
            raise ValueError(f"Ongeldige tag selector: '{tag}' (verwacht key=value)")

        key, value = tag.split("=", 1)
        selectors.append(TagSelector(key, value))

    if not selectors:
        raise ValueError("Minstens één selector is verplicht (--name of --tag)")

    if len(selectors) == 1:
        return selectors[0]

    return OrSelector(selectors) if use_or else AndSelector(selectors)

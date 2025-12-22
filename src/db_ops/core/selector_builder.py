from typing import Iterable

from db_ops.core.selectors import (
    JobSelector,
    NameRegexSelector,
    TagSelector,
    AndSelector,
    OrSelector,
)


def build_selector(
    *,
    name: str | None,
    tags: Iterable[str],
    use_or: bool,
) -> JobSelector:
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
from types import SimpleNamespace

import pytest

from dbops.core.catalog import (
    delete_schema_with_tables,
    delete_tables,
    drop_empty_schemas,
    find_empty_schemas,
    parse_schema_full_name,
)
from dbops.core.uc import UCTable


@pytest.mark.parametrize("value", ["main", "main.sales.tmp", "main.", ".sales", ""])
def test_parse_schema_full_name_rejects_invalid_input(value: str):
    with pytest.raises(ValueError, match="catalog.schema"):
        parse_schema_full_name(value)


def test_parse_schema_full_name_accepts_valid_input():
    assert parse_schema_full_name(" main.sales ") == ("main", "sales")


def test_delete_tables_dry_run_does_not_call_adapter():
    class _Adapter:
        def __init__(self):
            self.calls: list[str] = []

        def current_username(self) -> str:
            self.calls.append("current_username")
            return "me@example.com"

        def set_table_owner(self, full_name: str, owner: str) -> None:
            self.calls.append(f"set_table_owner:{full_name}:{owner}")

        def delete_table(self, full_name: str) -> None:
            self.calls.append(f"delete_table:{full_name}")

    adapter = _Adapter()
    results = delete_tables(adapter, ["main.sales.t1"], dry_run=True)

    assert adapter.calls == []
    assert len(results) == 1
    assert results[0].table == "main.sales.t1"
    assert results[0].owner_set is False
    assert results[0].deleted is False
    assert results[0].error is None


def test_delete_tables_collects_per_table_errors():
    class _Adapter:
        def current_username(self) -> str:
            return "me@example.com"

        def set_table_owner(self, full_name: str, owner: str) -> None:
            return None

        def delete_table(self, full_name: str) -> None:
            if full_name.endswith(".bad"):
                raise RuntimeError("boom")

    adapter = _Adapter()
    results = delete_tables(
        adapter,
        ["main.sales.good", "main.sales.bad"],
        dry_run=False,
    )

    by_table = {r.table: r for r in results}
    assert by_table["main.sales.good"].deleted is True
    assert by_table["main.sales.good"].error is None
    assert by_table["main.sales.bad"].deleted is False
    assert "boom" in (by_table["main.sales.bad"].error or "")


def test_delete_schema_with_tables_returns_table_results():
    class _Adapter:
        def __init__(self):
            self.schema_owner_set: list[tuple[str, str]] = []
            self.schema_deleted: list[tuple[str, bool]] = []

        def current_username(self) -> str:
            return "me@example.com"

        def list_tables(self, *, catalog: str, schema: str) -> list[UCTable]:
            assert catalog == "main"
            assert schema == "sales"
            return [
                UCTable(full_name="main.sales.good"),
                UCTable(full_name="main.sales.bad"),
            ]

        def set_schema_owner(self, schema_full_name: str, owner: str) -> None:
            self.schema_owner_set.append((schema_full_name, owner))

        def set_table_owner(self, full_name: str, owner: str) -> None:
            return None

        def delete_table(self, full_name: str) -> None:
            if full_name.endswith(".bad"):
                raise RuntimeError("table delete failed")

        def delete_schema(self, schema_full_name: str, force: bool = False) -> None:
            self.schema_deleted.append((schema_full_name, force))

    adapter = _Adapter()
    result = delete_schema_with_tables(
        adapter,
        schema_full_name="main.sales",
        dry_run=False,
    )

    table_results = result["table_results"]
    assert result["schema"] == "main.sales"
    assert result["schema_deleted"] is True
    assert len(table_results) == 2
    assert any(getattr(r, "error", None) for r in table_results)
    assert adapter.schema_owner_set == [("main.sales", "me@example.com")]
    assert adapter.schema_deleted == [("main.sales", False)]


def test_find_empty_schemas_filters_on_regex():
    class _Adapter:
        def list_schemas(self, *, catalog: str):
            assert catalog == "main"
            return [
                SimpleNamespace(name="tmp_a", full_name="main.tmp_a"),
                SimpleNamespace(name="prod_a", full_name="main.prod_a"),
            ]

        def list_tables(self, *, catalog: str, schema: str):
            if schema == "tmp_a":
                return []
            return [SimpleNamespace(full_name=f"{catalog}.{schema}.t1")]

    adapter = _Adapter()
    assert find_empty_schemas(adapter, catalog="main", name_regex=r"^main\.tmp") == [
        "main.tmp_a"
    ]


def test_drop_empty_schemas_dry_run_skips_owner_lookup():
    class _Adapter:
        def __init__(self):
            self.calls: list[str] = []

        def current_username(self) -> str:
            self.calls.append("current_username")
            return "me@example.com"

        def set_schema_owner(self, schema_full_name: str, owner: str) -> None:
            self.calls.append(f"set_schema_owner:{schema_full_name}:{owner}")

        def delete_schema(self, schema_full_name: str, force: bool = False) -> None:
            self.calls.append(f"delete_schema:{schema_full_name}:{force}")

    adapter = _Adapter()
    results = drop_empty_schemas(adapter, ["main.s1"], dry_run=True)

    assert adapter.calls == []
    assert len(results) == 1
    assert results[0].schema_full_name == "main.s1"
    assert results[0].ok is True

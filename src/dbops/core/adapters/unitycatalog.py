from __future__ import annotations

from databricks.sdk import WorkspaceClient

from dbops.core.uc_models import UCCatalog, UCSchema, UCTable


class UnityCatalogAdapter:
    """Adapter around Databricks SDK Unity Catalog APIs (tables/schemas/current user)."""

    def __init__(self, client: WorkspaceClient) -> None:
        self.client = client

    def current_username(self) -> str:
        """Return the current principal username/email for ownership operations."""
        me = self.client.current_user.me()
        # SDK uses user_name for SCIM-style usernames (often email)
        if not getattr(me, "user_name", None):
            raise ValueError("Could not determine current user (user_name is empty).")
        return me.user_name

    def list_catalogs(self) -> list[UCCatalog]:
        """List all Unity Catalog catalogs visible to the current principal."""
        out: list[UCCatalog] = []
        for c in self.client.catalogs.list():
            name = getattr(c, "name", None)
            if not name:
                continue
            out.append(UCCatalog(name=name, owner=getattr(c, "owner", None)))
        return out

    def list_schemas(self, catalog: str) -> list[UCSchema]:
        """List schemas in a given catalog."""
        out: list[UCSchema] = []
        for s in self.client.schemas.list(catalog_name=catalog):
            name = getattr(s, "name", None)
            full_name = getattr(s, "full_name", None)
            catalog_name = getattr(s, "catalog_name", None) or catalog

            if not name and full_name:
                name = full_name.split(".")[-1]

            if not full_name and name:
                full_name = f"{catalog_name}.{name}"

            if not name or not full_name:
                continue

            out.append(
                UCSchema(
                    full_name=full_name,
                    name=name,
                    catalog_name=catalog_name,
                    owner=getattr(s, "owner", None),
                )
            )
        return out

    def list_tables(self, catalog: str, schema: str) -> list[UCTable]:
        """List tables in a given catalog.schema."""
        out: list[UCTable] = []
        for t in self.client.tables.list(catalog_name=catalog, schema_name=schema):
            # TableInfo has .full_name, .owner, .table_type (string/enum depending on SDK)
            full_name = getattr(t, "full_name", None)
            if not full_name:
                continue
            out.append(
                UCTable(
                    full_name=full_name,
                    owner=getattr(t, "owner", None),
                    table_type=str(getattr(t, "table_type", None))
                    if getattr(t, "table_type", None)
                    else None,
                )
            )
        return out

    def set_table_owner(self, full_name: str, owner: str) -> None:
        """Set table owner."""
        self.client.tables.update(full_name=full_name, owner=owner)

    def delete_table(self, full_name: str) -> None:
        """Delete a table by full name."""
        self.client.tables.delete(full_name=full_name)

    def set_schema_owner(self, schema_full_name: str, owner: str) -> None:
        """Set schema owner."""
        self.client.schemas.update(full_name=schema_full_name, owner=owner)

    def delete_schema(self, schema_full_name: str, force: bool = False) -> None:
        """Delete schema (optionally forced)."""
        self.client.schemas.delete(full_name=schema_full_name, force=force)

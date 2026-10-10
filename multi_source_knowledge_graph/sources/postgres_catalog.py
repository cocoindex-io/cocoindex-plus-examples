"""Rows of a catalog table in Postgres, one entity per row.

The row's content is not read here: the catalog is snapshotted into context
when the app starts (see ``catalog.py``), and a row's component passes the
snapshot's entry on as the shared entity component's arguments. The component
exists to hold the entity's node and to re-run when the row changes, so the
node picks up the new properties.
"""

from __future__ import annotations

from collections.abc import AsyncIterable
from dataclasses import dataclass

import asyncpg

import cocoindex as coco
from cocoindex.connectors import postgres

from sources.base import EntitySource


@dataclass
class CatalogRef:
    """A listing entry: the key and the row's last-writer transaction id."""

    key: str
    xmin: int


@dataclass(frozen=True)
class PostgresCatalog(EntitySource[CatalogRef]):
    pool: asyncpg.Pool
    table: str

    def __coco_memo_key__(self) -> object:
        # The pool is a connection, not an input: memoize on what names the source.
        return (self.name, self.table)

    def refs(self) -> AsyncIterable[tuple[coco.StableKey, CatalogRef]]:
        # Two columns per row. xmin changes whenever the row is updated, so it
        # is a free change token: no updated_at column to maintain.
        rows = postgres.PgTableSource(
            self.pool,
            table_name=self.table,
            columns=["key", "xmin"],
            row_type=CatalogRef,
        )
        return rows.fetch_rows().items(lambda ref: ref.key)

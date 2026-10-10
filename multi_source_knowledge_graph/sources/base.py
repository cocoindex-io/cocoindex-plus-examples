# © 2026 CocoIndex Inc. All rights reserved.
# SPDX-License-Identifier: LicenseRef-CocoIndex-Proprietary
"""The two source families, with their default processing.

A document source lists its items as refs and fetches one into a record; an
entity source lists the entities it knows as refs. The family base supplies
the rest: ``process`` (one memoized child component per ref — for a document,
fetch on a miss and hand the record to the graph; for an entity, hold its
shared node) and ``ingest`` (the source's own processing component, which
mounts those children). A source module writes ``refs()``, plus ``fetch()``
for documents, and overrides ``process`` only when its records need more than
that. Either way it ends in the graph's ``add_*`` methods, so the targets and
the write logic stay shared.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterable
from dataclasses import dataclass
from typing import Generic, Protocol, TypeVar

import cocoindex as coco

from graph import KnowledgeGraph
from records import Document

RefT = TypeVar("RefT")


class Source(Protocol):
    """What the registry needs: a name, and the ability to ingest into a graph."""

    @property
    def name(self) -> str: ...

    async def ingest(self, graph: KnowledgeGraph) -> None: ...


@dataclass(frozen=True)
class DocumentSource(ABC, Generic[RefT]):
    name: str

    @abstractmethod
    def refs(self) -> AsyncIterable[tuple[coco.StableKey, RefT]]:
        """List items cheaply; each ref carries a change token."""

    @abstractmethod
    async def fetch(self, ref: RefT) -> Document:
        """Load one item. Runs only when ``process`` misses its memo."""

    @coco.fn(memo=True)
    async def process(self, ref: RefT, graph: KnowledgeGraph) -> None:
        await graph.add_document(await self.fetch(ref))

    @coco.fn
    async def ingest(self, graph: KnowledgeGraph) -> None:
        await coco.mount_each(
            coco.component_subpath("record"), self.process, self.refs(), graph
        )


class EntityRef(Protocol):
    """What an entity source lists: the entity's key, plus a change token."""

    @property
    def key(self) -> str: ...


EntityRefT = TypeVar("EntityRefT", bound=EntityRef)


@dataclass(frozen=True)
class EntitySource(ABC, Generic[EntityRefT]):
    name: str

    @abstractmethod
    def refs(self) -> AsyncIterable[tuple[coco.StableKey, EntityRefT]]:
        """List entities cheaply; each ref carries a change token."""

    @coco.fn(memo=True)
    async def process(self, ref: EntityRefT, graph: KnowledgeGraph) -> None:
        await graph.add_entity(ref.key)

    @coco.fn
    async def ingest(self, graph: KnowledgeGraph) -> None:
        await coco.mount_each(
            coco.component_subpath("record"), self.process, self.refs(), graph
        )

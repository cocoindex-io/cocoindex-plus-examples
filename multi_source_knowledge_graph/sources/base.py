"""The source families, with their default processing.

A source lists its items as refs; its family says what to do with one. The
``Source`` base supplies ``ingest``, the source's own processing component,
which mounts one memoized child component per ref — the family's ``process``.
A document source fetches the ref into a record on a memo miss and hands it to
the graph; an entity source mounts the entity's shared node. A source module
writes ``refs()``, plus ``fetch()`` for documents, and overrides ``process``
only when its records need more than that. Either way it ends in the graph's
``declare_*`` methods, so the targets and the write logic stay shared.
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


@dataclass(frozen=True)
class Source(ABC, Generic[RefT]):
    name: str

    @abstractmethod
    def refs(self) -> AsyncIterable[tuple[coco.StableKey, RefT]]:
        """List items cheaply; each ref carries a change token."""

    @abstractmethod
    async def process(self, ref: RefT, graph: KnowledgeGraph) -> None:
        """One memoized child component per ref; the family supplies it."""

    @coco.fn
    async def ingest(self, graph: KnowledgeGraph) -> None:
        await coco.mount_each(
            coco.component_subpath("record"), self.process, self.refs(), graph
        )


@dataclass(frozen=True)
class DocumentSource(Source[RefT]):
    @abstractmethod
    async def fetch(self, ref: RefT) -> Document:
        """Load one item. Runs only when ``process`` misses its memo."""

    @coco.fn(memo=True)
    async def process(self, ref: RefT, graph: KnowledgeGraph) -> None:
        await graph.declare_document(await self.fetch(ref))


class EntityRef(Protocol):
    """What an entity source lists: the entity's key, plus a change token."""

    @property
    def key(self) -> str: ...


@dataclass(frozen=True)
class EntitySource(Source[EntityRef]):
    @coco.fn(memo=True)
    async def process(self, ref: EntityRef, graph: KnowledgeGraph) -> None:
        await graph.declare_entity(ref.key)

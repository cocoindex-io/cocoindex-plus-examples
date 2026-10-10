"""The shared phase: the targets every source writes to, the nodes several
sources hold together, and the steps every record goes through on its way in.

``KnowledgeGraph`` is mounted once and handed to every source. A source hands
it records; it resolves their references, declares the nodes and edges the
record owns outright, and mounts the shared components for the nodes the
record only holds a share of. The ``add_*`` methods are ``@coco.fn`` so an
edit to this file re-syncs every source through logic tracking.

Ownership, node by node:

- A ``Document`` node, its ``MENTIONS`` edges and its ``AUTHORED`` edge belong
  to the document's own component: they come and go with the record.
- An ``Entity`` node is a shared component keyed by the entity key. Its
  holders are the catalog row that describes it and every document that
  mentions it; the node exists while any holder remains. Its properties and
  ``DEPENDS_ON`` edges are the component's arguments, which every holder
  takes from the same catalog snapshot — the dependencies resolved through
  the resolver, like a document's references — so all holders mount it the
  same way, and a changed row, or a dependency joining or leaving the
  catalog, re-mounts it with changed arguments.
- A ``Person`` node is a shared component keyed by login, held by every
  document the person authored.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any

import cocoindex as coco
from cocoindex.connectors import neo4j

from catalog import ENTRIES, RESOLVER
from records import Document, Entity


# ---------------------------------------------------------------------------
# Node schemas (dataclasses for declare_record)
# ---------------------------------------------------------------------------


@dataclass
class EntityNode:
    key: str  # primary key
    name: str
    kind: str
    area: str


@dataclass
class DocumentNode:
    id: str  # primary key — "<source>:<key>"
    source: str
    kind: str
    title: str
    url: str
    status: str
    updated_at: str


@dataclass
class PersonNode:
    login: str  # primary key


# DEPENDS_ON, MENTIONS and AUTHORED carry no payload — declared without a
# schema, so the connector derives each edge's identity from its endpoints.


class KnowledgeGraph:
    """Entity, Document and Person nodes with their edges, shared by all sources."""

    def __init__(
        self,
        entity_table: neo4j.TableTarget[EntityNode],
        document_table: neo4j.TableTarget[DocumentNode],
        person_table: neo4j.TableTarget[PersonNode],
        depends_on_rel: neo4j.RelationTarget[Any],
        mentions_rel: neo4j.RelationTarget[Any],
        authored_rel: neo4j.RelationTarget[Any],
    ) -> None:
        self._entity_table = entity_table
        self._document_table = document_table
        self._person_table = person_table
        self._depends_on_rel = depends_on_rel
        self._mentions_rel = mentions_rel
        self._authored_rel = authored_rel

    @classmethod
    async def mount(
        cls, db: coco.ContextKey[neo4j.ConnectionFactory]
    ) -> KnowledgeGraph:
        entity_table = await neo4j.mount_table_target(
            db,
            "Entity",
            await neo4j.TableSchema.from_class(EntityNode, primary_key="key"),
            primary_key="key",
        )
        document_table = await neo4j.mount_table_target(
            db,
            "Document",
            await neo4j.TableSchema.from_class(DocumentNode, primary_key="id"),
            primary_key="id",
        )
        person_table = await neo4j.mount_table_target(
            db,
            "Person",
            await neo4j.TableSchema.from_class(PersonNode, primary_key="login"),
            primary_key="login",
        )
        depends_on_rel = await neo4j.mount_relation_target(
            db, "DEPENDS_ON", entity_table, entity_table
        )
        mentions_rel = await neo4j.mount_relation_target(
            db, "MENTIONS", document_table, entity_table
        )
        authored_rel = await neo4j.mount_relation_target(
            db, "AUTHORED", person_table, document_table
        )
        return cls(
            entity_table,
            document_table,
            person_table,
            depends_on_rel,
            mentions_rel,
            authored_rel,
        )

    def __coco_memo_key__(self) -> object:
        return (
            self._entity_table,
            self._document_table,
            self._person_table,
            self._depends_on_rel,
            self._mentions_rel,
            self._authored_rel,
        )

    # --- Shared components: one instance per key, held by every mounter ------

    @coco.fn(memo=True)
    def entity_node(self, entity: Entity) -> None:
        """The Entity node and its DEPENDS_ON edges, a function of its arguments."""
        self._entity_table.declare_record(
            row=EntityNode(
                key=entity.key, name=entity.name, kind=entity.kind, area=entity.area
            )
        )
        for dep in entity.depends_on:
            self._depends_on_rel.declare_relation(from_id=entity.key, to_id=dep)

    @coco.fn(memo=True)
    def person_node(self, login: str) -> None:
        """The Person node. Identity is the login; nothing else is shared."""
        self._person_table.declare_record(row=PersonNode(login=login))

    # --- Write steps: what a source's record contributes ---------------------

    async def _hold_entity(self, key: str) -> None:
        # Every holder passes the same arguments: the catalog's entry, with its
        # dependencies resolved through the resolver like a document's refs, so
        # a dependency joining or leaving the catalog re-mounts the entity.
        resolver = coco.use_context(RESOLVER)
        entity = coco.use_context(ENTRIES)[key]
        depends_on = tuple(dep for dep in entity.depends_on if dep in resolver.keys)
        await coco.use_mount_shared(
            ("entity", key), self.entity_node, replace(entity, depends_on=depends_on)
        )

    @coco.fn
    async def add_entity(self, key: str) -> None:
        """Hold the shared Entity node for ``key``."""
        await self._hold_entity(key)

    @coco.fn
    async def add_document(self, doc: Document) -> None:
        """Declare the Document node; hold and link every entity it mentions
        and the person who wrote it."""
        doc_id = f"{doc.source}:{doc.key}"
        self._document_table.declare_record(
            row=DocumentNode(
                id=doc_id,
                source=doc.source,
                kind=doc.kind,
                title=doc.title,
                url=doc.url,
                status=doc.status,
                updated_at=doc.updated_at,
            )
        )
        resolver = coco.use_context(RESOLVER)
        mentioned = resolver.resolve(f"{doc.title}\n{doc.text}")
        mentioned.update(ref for ref in doc.refs if ref in resolver.keys)
        for key in sorted(mentioned):
            await self._hold_entity(key)
            self._mentions_rel.declare_relation(from_id=doc_id, to_id=key)
        if doc.author:
            await coco.use_mount_shared(
                ("person", doc.author), self.person_node, doc.author
            )
            self._authored_rel.declare_relation(from_id=doc.author, to_id=doc_id)

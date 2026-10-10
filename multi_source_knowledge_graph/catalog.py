"""The catalog, snapshotted into two context values when the app starts.

``Resolver`` maps free text to entity keys by whole-word alias matching. It is
the entity-resolution seam of the pipeline: replace it with an embedding or
LLM resolver — or a call to an external oracle — and nothing else changes.
``ENTRIES`` holds what the catalog says about each entity, by key: the
arguments every owner passes when it mounts the entity's shared component.

The two differ in change detection, so that each kind of catalog edit re-runs
only the components that must see it. The resolver is change-detected: an
alias edit re-runs every document, because each resolved against the aliases.
The entries are not: a property edit is detected by the row's ``xmin``, which
re-runs that row's component, which re-mounts the shared entity with the new
properties as its arguments. Change detection on the entries would make every
owner — every document that mentions an entity — depend on the whole catalog,
since a memoized component inherits the dependencies of what it mounts.
"""

from __future__ import annotations

import functools
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass

import cocoindex as coco

from records import Entity


@dataclass
class CatalogRow:
    """A full row of the catalog table."""

    key: str
    name: str
    kind: str
    area: str
    aliases: list[str]
    depends_on: list[str]


@dataclass(frozen=True)
class Resolver:
    """Every way an entity may be written, mapped to its key, and the pattern
    that finds them in text.

    Both are built here, at snapshot time, so the matching rule is part of the
    change-detected value: editing it re-processes every document, as editing an
    alias does.
    """

    aliases: tuple[tuple[str, str], ...]  # (alias, entity key), sorted
    pattern: str  # whole-word regex over every alias, longest first

    @classmethod
    def from_rows(cls, rows: Iterable[CatalogRow]) -> Resolver:
        pairs = {
            (alias.strip().lower(), row.key)
            for row in rows
            for alias in (row.key, row.name, *row.aliases)
            if alias.strip()
        }
        # Longest alias first so "oci object storage" wins over "oci"; ties in
        # alphabetical order, so the pattern — part of the change-detected
        # value — is the same string in every process. Letters and digits
        # delimit a word; "_" and "-" do not, so "postgres_target" and
        # "amazon-s3" still match.
        alternatives = "|".join(
            re.escape(alias)
            for alias in sorted(
                {alias for alias, _ in pairs}, key=lambda alias: (-len(alias), alias)
            )
        )
        return cls(
            aliases=tuple(sorted(pairs)),
            pattern=rf"(?<![a-z0-9])(?:{alternatives})(?![a-z0-9])",
        )

    def __coco_memo_key__(self) -> object:
        return (self.aliases, self.pattern)

    @functools.cached_property
    def _lookup(self) -> dict[str, str]:
        return dict(self.aliases)

    @functools.cached_property
    def keys(self) -> frozenset[str]:
        return frozenset(self._lookup.values())

    @coco.fn
    def resolve(self, text: str) -> set[str]:
        """Entity keys mentioned in ``text``."""
        if not self.aliases:
            return set()  # an empty alternation would match the empty string
        # re caches compiled patterns, so compiling by source string here is free.
        return {
            self._lookup[m.group(0)] for m in re.finditer(self.pattern, text.lower())
        }


def entries_from_rows(rows: Iterable[CatalogRow]) -> dict[str, Entity]:
    """What the catalog says about each entity, by key."""
    return {
        row.key: Entity(
            key=row.key,
            name=row.name,
            kind=row.kind,
            area=row.area,
            depends_on=tuple(row.depends_on),
        )
        for row in rows
    }


RESOLVER = coco.ContextKey[Resolver]("catalog_resolver", detect_change=True)
ENTRIES = coco.ContextKey[Mapping[str, Entity]]("catalog_entries")

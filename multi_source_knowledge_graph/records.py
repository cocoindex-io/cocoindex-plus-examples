# © 2026 CocoIndex Inc. All rights reserved.
# SPDX-License-Identifier: LicenseRef-CocoIndex-Proprietary
"""The records a source produces, and the entities they refer to.

Documents mention entities and name their authors; entities are what the
catalog describes.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Document:
    """One item of any document source."""

    source: str
    key: str  # stable within the source
    kind: str  # "doc" | "issue" | "pull_request" | "release"
    title: str
    text: str
    url: str
    author: str  # the author's login, or "" when the source has none
    status: str
    updated_at: str
    # Entity keys the source already knows structurally, on top of what the
    # resolver finds in the text (e.g. the scope of a conventional-commit title).
    refs: tuple[str, ...] = ()


@dataclass(frozen=True)
class Entity:
    """One entity as the catalog describes it."""

    key: str
    name: str
    kind: str
    area: str
    depends_on: tuple[str, ...] = ()

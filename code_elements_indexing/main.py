# © 2025 CocoIndex Inc. All rights reserved.
# SPDX-License-Identifier: LicenseRef-CocoIndex-Proprietary
"""
Code Elements Indexing (v1) — CocoIndex pipeline example.

Walks a GitHub repository via a GitHub App installation, runs the
TreeSitter-based ``ExtractCodeElements`` over each ``.py`` / ``.cs`` file,
and writes one row per declaration and one row per call/type reference into
two Postgres tables.

Indexing (catch-up — one pass and exit):
    cocoindex update main

Indexing (live — re-poll the ref every 10 seconds, see ``sync_github_repo``):
    cocoindex update -L main

Environment:
    GITHUB_APP_ID         — your GitHub App ID
    GITHUB_PRIVATE_KEY_PATH — filesystem path to the App's PEM private key
    GITHUB_REPO_OWNER     — repo owner (defaults to ``cocoindex-io``)
    GITHUB_REPO_NAME      — repo name  (defaults to ``cocoindex``)
    GITHUB_REPO_REF       — git ref    (defaults to ``main``)
    POSTGRES_URL          — connection string for the target database
"""

from __future__ import annotations

import datetime
import os
import uuid
from dataclasses import dataclass
from dotenv import load_dotenv
from typing import AsyncIterator

import asyncpg

import cocoindex as coco
from cocoindex.connectors import github, postgres
from cocoindex.ops.code_ast import ExtractCodeElements
from cocoindex.ops.text import detect_code_language
from cocoindex.resources.file import PatternFilePathMatcher
from cocoindex.resources.id import UuidGenerator


DATABASE_URL = os.getenv(
    "POSTGRES_URL", "postgres://cocoindex:cocoindex@localhost/cocoindex"
)
DECLARATIONS_TABLE = "code_declarations"
REFERENCES_TABLE = "code_references"
PG_SCHEMA_NAME = "coco_examples"

PG_DB = coco.ContextKey[asyncpg.Pool]("code_elements_db")

# `ExtractCodeElements()` with no arg uses the built-in defaults
# (Python + C#). Build once and reuse across all files.
_extractor = ExtractCodeElements()


@dataclass
class DeclarationRow:
    id: uuid.UUID
    filename: str
    language: str
    namespace: str
    entity_name: str
    parent_entity_name: str | None
    base_name: str
    ast_node_kind: str
    has_body: bool
    start_line: int
    start_column: int
    start_char_offset: int
    end_line: int
    end_column: int
    end_char_offset: int


@dataclass
class ReferenceRow:
    id: uuid.UUID
    filename: str
    language: str
    namespace: str
    parent_entity_name: str | None
    referenced_base_name: str
    referenced_full_path: str
    ast_node_kind: str
    start_line: int
    start_column: int
    start_char_offset: int
    end_line: int
    end_column: int
    end_char_offset: int


@coco.lifespan
async def coco_lifespan(
    builder: coco.EnvironmentBuilder,
) -> AsyncIterator[None]:
    async with asyncpg.create_pool(DATABASE_URL) as pool:
        builder.provide(PG_DB, pool)
        yield


@coco.fn
async def process_file(
    file: github.File,
    declarations_table: postgres.TableTarget[DeclarationRow],
    references_table: postgres.TableTarget[ReferenceRow],
) -> None:
    filename = str(file.file_path.path)
    language = detect_code_language(filename=file.file_path.path.name)
    if language is None:
        return

    text = await file.read_text()
    elements = _extractor.extract(text, language=language)

    # Per-file UUID generators: keyed by the file path so the sequence of
    # UUIDs is stable across runs even when two declarations share all
    # metadata except position.
    decl_ids = UuidGenerator(("decl", filename))
    ref_ids = UuidGenerator(("ref", filename))

    for d in elements.declarations:
        declarations_table.declare_row(
            row=DeclarationRow(
                id=decl_ids.next_uuid(
                    (d.namespace, d.entity_name, d.ast_node_kind, d.start.char_offset)
                ),
                filename=filename,
                language=language,
                namespace=d.namespace,
                entity_name=d.entity_name,
                parent_entity_name=d.parent_entity_name,
                base_name=d.base_name,
                ast_node_kind=d.ast_node_kind,
                has_body=d.has_body,
                start_line=d.start.line,
                start_column=d.start.column,
                start_char_offset=d.start.char_offset,
                end_line=d.end.line,
                end_column=d.end.column,
                end_char_offset=d.end.char_offset,
            ),
        )

    for r in elements.references:
        references_table.declare_row(
            row=ReferenceRow(
                id=ref_ids.next_uuid(
                    (
                        r.namespace,
                        r.parent_entity_name,
                        r.referenced_full_path,
                        r.ast_node_kind,
                        r.start.char_offset,
                    )
                ),
                filename=filename,
                language=language,
                namespace=r.namespace,
                parent_entity_name=r.parent_entity_name,
                referenced_base_name=r.referenced_base_name,
                referenced_full_path=r.referenced_full_path,
                ast_node_kind=r.ast_node_kind,
                start_line=r.start.line,
                start_column=r.start.column,
                start_char_offset=r.start.char_offset,
                end_line=r.end.line,
                end_column=r.end.column,
                end_char_offset=r.end.char_offset,
            ),
        )


async def sync_github_repo(
    owner: str,
    repo: str,
    ref: str | None,
    declarations_table: postgres.TableTarget[DeclarationRow],
    references_table: postgres.TableTarget[ReferenceRow],
) -> None:
    """One pass: resolve `ref` to the current commit, walk it, mount each
    matching file.

    Wrapped in ``coco.auto_refresh`` by the caller: catch-up mode runs once,
    live mode re-invokes on a fixed interval so the index follows the ref
    forward. SHA-keyed memoization means unchanged blobs are not re-read or
    re-parsed between cycles.
    """
    async with github.GitHubRepo(
        app=github.GitHubApp(
            app_id=int(os.environ["GITHUB_APP_ID"]),
            private_key_path=os.environ["GITHUB_PRIVATE_KEY_PATH"],
        ),
        owner=owner,
        repo=repo,
    ) as gh_repo:
        commit = await gh_repo.get_commit(ref=ref)

        await github.mount_each_file(
            process_file,
            commit,
            github.WalkOptions(
                path_matcher=PatternFilePathMatcher(
                    included_patterns=["**/*.py", "**/*.cs"],
                    excluded_patterns=["**/.*"],
                ),
            ),
            declarations_table,
            references_table,
        )


@coco.fn
async def app_main(owner: str, repo: str, ref: str | None) -> None:
    declarations_table = await postgres.mount_table_target(
        PG_DB,
        table_name=DECLARATIONS_TABLE,
        table_schema=await postgres.TableSchema.from_class(
            DeclarationRow,
            primary_key=["id"],
        ),
        pg_schema_name=PG_SCHEMA_NAME,
    )
    references_table = await postgres.mount_table_target(
        PG_DB,
        table_name=REFERENCES_TABLE,
        table_schema=await postgres.TableSchema.from_class(
            ReferenceRow,
            primary_key=["id"],
        ),
        pg_schema_name=PG_SCHEMA_NAME,
    )

    # `coco.auto_refresh` runs once in catch-up mode and re-runs every
    # 10 seconds in live mode (`cocoindex update -L`).
    await coco.mount(
        coco.auto_refresh(sync_github_repo, interval=datetime.timedelta(seconds=10)),
        owner,
        repo,
        ref,
        declarations_table,
        references_table,
    )


load_dotenv()

app = coco.App(
    coco.AppConfig(name="CodeElementsIndexing"),
    app_main,
    owner=os.environ.get("GITHUB_REPO_OWNER", "cocoindex-io"),
    repo=os.environ.get("GITHUB_REPO_NAME", "cocoindex"),
    ref=os.environ.get("GITHUB_REPO_REF", "main"),
)


if __name__ == "__main__":
    app.update_blocking(report_to_stdout=True)

# © 2025 CocoIndex Inc. All rights reserved.
# SPDX-License-Identifier: LicenseRef-CocoIndex-Proprietary
"""
GitHub Code Indexing (v1) — CocoIndex pipeline example.

Walks a GitHub repository via a GitHub App installation, chunks code files,
embeds each chunk with a SentenceTransformer model, and writes vectors to
Postgres / pgvector.

Indexing (catch-up — one pass and exit):
    cocoindex update main

Indexing (live — re-poll the ref every 5 minutes, see ``sync_github_repo``):
    cocoindex update -L main

Query:
    python main.py "your query"

Environment:
    GITHUB_APP_ID         — your GitHub App ID
    GITHUB_PRIVATE_KEY_PATH — filesystem path to the App's PEM private key
    POSTGRES_URL          — connection string for the target database
"""

from __future__ import annotations

import asyncio
import datetime
import os
import pathlib
import sys
from dataclasses import dataclass
from dotenv import load_dotenv
from typing import Annotated, AsyncIterator

import asyncpg
from pgvector.asyncpg import register_vector
import numpy as np
from cocoindex.resources.schema import VectorSchema
from cocoindex.resources.rate_limit import RateLimiter
from numpy.typing import NDArray

import cocoindex as coco
from cocoindex.connectors import github, postgres
from cocoindex.ops.text import RecursiveSplitter, detect_code_language
from cocoindex.ops.sentence_transformers import SentenceTransformerEmbedder
from cocoindex.resources.chunk import Chunk
from cocoindex.resources.file import PatternFilePathMatcher
from cocoindex.resources.id import IdGenerator


DATABASE_URL = os.getenv(
    "POSTGRES_URL", "postgres://cocoindex:cocoindex@localhost/cocoindex"
)
# TABLE_NAME = "github_code_indexing"
TABLE_NAME = "github_code_indexing_mini"
PG_SCHEMA_NAME = "coco_examples"
TOP_K = 5

EMBED_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
PG_DB = coco.ContextKey[asyncpg.Pool]("github_code_embedding_db")
EMBEDDER = coco.ContextKey[SentenceTransformerEmbedder]("embedder", detect_change=True)

_splitter = RecursiveSplitter()


@dataclass
class CodeEmbedding:
    id: int
    filename: str
    code: str
    embedding: Annotated[NDArray, VectorSchema(dtype=np.dtype(np.float16), size=384)]
    start_line: int
    end_line: int


@coco.lifespan
async def coco_lifespan(
    builder: coco.EnvironmentBuilder,
) -> AsyncIterator[None]:
    async with asyncpg.create_pool(DATABASE_URL) as pool:
        builder.provide(PG_DB, pool)
        builder.provide(EMBEDDER, SentenceTransformerEmbedder(EMBED_MODEL))
        yield


@coco.fn
async def process_chunk(
    chunk: Chunk,
    filename: pathlib.PurePath,
    id_gen: IdGenerator,
    table: postgres.TableTarget[CodeEmbedding],
) -> None:
    embedding = await coco.use_context(EMBEDDER).embed(chunk.text)
    table.declare_row(
        row=CodeEmbedding(
            id=await id_gen.next_id(chunk.text),
            filename=str(filename),
            code=chunk.text,
            embedding=embedding,
            start_line=chunk.start.line,
            end_line=chunk.end.line,
        ),
    )


@coco.fn
async def process_file(
    file: github.File,
    table: postgres.TableTarget[CodeEmbedding],
) -> None:
    text = await file.read_text()
    language = detect_code_language(filename=file.file_path.path.name)
    chunks = _splitter.split(
        text,
        chunk_size=1000,
        min_chunk_size=300,
        chunk_overlap=300,
        language=language,
    )
    id_gen = IdGenerator()
    await coco.map(process_chunk, chunks, file.file_path.path, id_gen, table)


async def sync_github_repo(
    owner: str,
    repo: str,
    ref: str | None,
    target_table: postgres.TableTarget[CodeEmbedding],
) -> None:
    """One pass: resolve `ref` to the current commit, walk it, mount each
    matching file.

    Used directly under `coco.auto_refresh` below: in catch-up mode it runs
    once; in live mode the wrapper re-invokes it on a fixed interval, picking
    up new commits at the same ref. SHA-keyed memoization means blobs whose
    content hasn't changed between polls are not re-read or re-embedded.
    """
    async with github.GitHubApp(
        app_id=int(os.environ["GITHUB_APP_ID"]),
        private_key_path=os.environ["GITHUB_PRIVATE_KEY_PATH"],
        rate_limiter=RateLimiter(max_rows_per_second=1.0),
    ) as app:
        gh_repo = github.GitHubRepo(
            app=app,
            owner=owner,
            repo=repo,
        )
        commit = await gh_repo.get_commit(ref=ref)

        await github.mount_each_file(
            process_file,
            commit,
            github.WalkOptions(
                path_matcher=PatternFilePathMatcher(
                    included_patterns=[
                        "**/*.py",
                        "**/*.h",
                        "**/*.cpp",
                        "**/*.c",
                        "**/*.rs",
                        "**/*.toml",
                        "**/*.md",
                        "**/*.mdx",
                    ],
                    excluded_patterns=["**/.*", "**/target", "**/node_modules"],
                ),
            ),
            target_table,
        )


@coco.fn
async def app_main(owner: str, repo: str, ref: str | None) -> None:
    target_table = await postgres.mount_table_target(
        PG_DB,
        table_name=TABLE_NAME,
        table_schema=await postgres.TableSchema.from_class(
            CodeEmbedding,
            primary_key=["id"],
        ),
        pg_schema_name=PG_SCHEMA_NAME,
    )
    target_table.declare_vector_index(column="embedding")

    # `coco.auto_refresh` makes a single pass under catch-up mode and
    # re-runs every 10 seconds under live mode (`cocoindex update -L`),
    # so the index follows the ref forward without an explicit watcher.
    await coco.mount(
        coco.auto_refresh(sync_github_repo, interval=datetime.timedelta(seconds=10)),
        owner,
        repo,
        ref,
        target_table,
    )


# app = coco.App(
#     coco.AppConfig(name="GitHubCodeIndexing"),
#     app_main,
#     owner="georgeh0",
#     repo="llvm-project",
#     ref="main",
# )

app = coco.App(
    coco.AppConfig(name="GitHubCodeIndexing_mini"),
    app_main,
    owner="cocoindex-io",
    repo="cocoindex",
    ref=None,
)

# ============================================================================
# Query demo
# ============================================================================


async def query_once(
    pool: asyncpg.Pool,
    embedder: SentenceTransformerEmbedder,
    query: str,
    *,
    top_k: int = TOP_K,
) -> None:
    query_vec = await embedder.embed(query)
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT
                filename,
                code,
                embedding <=> $1 AS distance,
                start_line,
                end_line
            FROM "{PG_SCHEMA_NAME}"."{TABLE_NAME}"
            ORDER BY distance ASC
            LIMIT $2
            """,
            query_vec,
            top_k,
        )

    for r in rows:
        score = 1.0 - float(r["distance"])
        print(f"[{score:.3f}] {r['filename']} (L{r['start_line']}-L{r['end_line']})")
        print(f"    {r['code']}")
        print("---")


async def query(initial_query: str | None = None) -> None:
    embedder = SentenceTransformerEmbedder(EMBED_MODEL)
    async with asyncpg.create_pool(DATABASE_URL, init=register_vector) as pool:
        if initial_query is not None:
            await query_once(pool, embedder, initial_query)
            return

        while True:
            q = input("Enter search query (or Enter to quit): ").strip()
            if not q:
                break
            await query_once(pool, embedder, q)


if __name__ == "__main__":
    load_dotenv()
    initial = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else None
    asyncio.run(query(initial))

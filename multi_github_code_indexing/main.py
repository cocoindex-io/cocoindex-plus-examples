# © 2025 CocoIndex Inc. All rights reserved.
# SPDX-License-Identifier: LicenseRef-CocoIndex-Proprietary
"""
Multi-tenant GitHub Code Indexing (v1) — CocoIndex pipeline example.

Indexes many GitHub repositories at once, one *tenant* per entry in a JSON
config file. Tenant configs are picked up live: edit ``example_configs/*.json``
and the affected tenants are added/removed without restarting the app.

Compared with v0, v1 doesn't need a separate "meta flow" + custom
``TargetSpec`` to manage tenants. The component tree gives us that for
free: a tenant key is just a component subpath under the config-file
component. Adding a key in the JSON creates a new component; removing a
key drops the component (and its rows). Changing a key's config
re-mounts the tenant with the new parameters.

Indexing (catch-up):
    cocoindex update main

Indexing (live — watches the config dir AND polls each tenant every 5 minutes):
    cocoindex update -L main

Query:
    python main.py "your query"

Environment:
    GITHUB_APP_ID         — your GitHub App ID
    GITHUB_PRIVATE_KEY_PATH — filesystem path to the App's PEM private key
    POSTGRES_URL          — connection string for the target database

Note on rate limiting: v1's GitHub connector doesn't yet have an
in-process throttle, so several tenants walking the same App in parallel
can burst against the GitHub API. The 429-retry loop will recover, but
if you have many tenants consider staggering the auto_refresh intervals
or running fewer concurrent tenants.
"""

from __future__ import annotations

import asyncio
import datetime
import json
import os
import pathlib
import sys
from dataclasses import dataclass
from dotenv import load_dotenv
from typing import Annotated, Any, AsyncIterator

import asyncpg
from pgvector.asyncpg import register_vector
from numpy.typing import NDArray

import cocoindex as coco
from cocoindex.connectors import github, localfs, postgres
from cocoindex.ops.text import RecursiveSplitter, detect_code_language
from cocoindex.ops.sentence_transformers import SentenceTransformerEmbedder
from cocoindex.resources.chunk import Chunk
from cocoindex.resources.file import PatternFilePathMatcher
from cocoindex.resources.id import IdGenerator
from cocoindex.resources.rate_limit import RateLimiter


DATABASE_URL = os.getenv(
    "POSTGRES_URL", "postgres://cocoindex:cocoindex@localhost/cocoindex"
)
TABLE_NAME = "multi_github_code_indexing"
PG_SCHEMA_NAME = "coco_examples"
TOP_K = 5

EMBED_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
PG_DB = coco.ContextKey[asyncpg.Pool]("multi_github_code_embedding_db")
EMBEDDER = coco.ContextKey[SentenceTransformerEmbedder]("embedder", detect_change=True)
GITHUB_APP = coco.ContextKey[github.GitHubApp]("github_app")

_splitter = RecursiveSplitter()


# ---------------------------------------------------------------------------
# Per-tenant config
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _RepoConfig:
    """One tenant's worth of repo coordinates + filter rules.

    Encoded directly as a row in the JSON config file. ``to_delete`` is the
    sentinel used by callers to remove a tenant — we skip it in
    ``parse_tenant_configs`` so the tenant never gets mounted.
    """

    repo_owner: str
    repo_name: str
    git_ref: str
    included_patterns: list[str] | None = None
    excluded_patterns: list[str] | None = None


def parse_tenant_configs(text: str) -> dict[str, _RepoConfig]:
    """Parse a config file body into ``{tenant_key: _RepoConfig}``.

    ``to_delete: true`` entries are filtered out so the corresponding
    component never gets mounted on this run — CocoIndex's standard
    cleanup then removes the tenant's rows.
    """
    raw: dict[str, dict[str, Any]] = json.loads(text)
    out: dict[str, _RepoConfig] = {}
    for tenant_key, cfg in raw.items():
        if cfg.get("to_delete"):
            continue
        out[tenant_key] = _RepoConfig(
            repo_owner=cfg["repo_owner"],
            repo_name=cfg["repo_name"],
            git_ref=cfg["git_ref"],
            included_patterns=cfg.get("included_patterns"),
            excluded_patterns=cfg.get("excluded_patterns"),
        )
    return out


# ---------------------------------------------------------------------------
# Schema + lifespan
# ---------------------------------------------------------------------------


@dataclass
class CodeEmbedding:
    id: int
    tenant_key: str
    filename: str
    code: str
    embedding: Annotated[NDArray, EMBEDDER]
    start_line: int
    end_line: int


@coco.lifespan
async def coco_lifespan(
    builder: coco.EnvironmentBuilder,
) -> AsyncIterator[None]:
    await builder.provide_async_with(PG_DB, asyncpg.create_pool(DATABASE_URL))
    builder.provide(EMBEDDER, SentenceTransformerEmbedder(EMBED_MODEL))
    await builder.provide_async_with(
        GITHUB_APP,
        github.GitHubApp(
            app_id=int(os.environ["GITHUB_APP_ID"]),
            private_key_path=os.environ["GITHUB_PRIVATE_KEY_PATH"],
            rate_limiter=RateLimiter(max_rows_per_second=1.0),
        ),
    )
    yield


# ---------------------------------------------------------------------------
# Per-file processing (innermost layer)
# ---------------------------------------------------------------------------


@coco.fn
async def process_chunk(
    chunk: Chunk,
    tenant_key: str,
    filename: pathlib.PurePath,
    id_gen: IdGenerator,
    table: postgres.TableTarget[CodeEmbedding],
) -> None:
    embedding = await coco.use_context(EMBEDDER).embed(chunk.text)
    table.declare_row(
        row=CodeEmbedding(
            id=await id_gen.next_id(chunk.text),
            tenant_key=tenant_key,
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
    tenant_key: str,
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
    await coco.map(
        process_chunk, chunks, tenant_key, file.file_path.path, id_gen, table
    )


# ---------------------------------------------------------------------------
# Per-tenant + per-config-file orchestration
# ---------------------------------------------------------------------------


async def sync_tenant_repo(
    tenant_key: str,
    config: _RepoConfig,
    target_table: postgres.TableTarget[CodeEmbedding],
) -> None:
    """One refresh cycle for one tenant: resolve the configured ref, walk
    the tree, mount each matching file as a per-file processing component.

    Wrapped in ``coco.auto_refresh`` by the caller so live mode polls
    GitHub on a fixed interval. SHA-keyed memoization means unchanged
    blobs are not re-read or re-embedded between cycles.
    """
    with coco.stats_group(f"tenant:{tenant_key}", report_to_stdout=True):
        gh_repo = github.GitHubRepo(
            app=coco.use_context(GITHUB_APP),
            owner=config.repo_owner,
            repo=config.repo_name,
        )
        commit = await gh_repo.get_commit(ref=config.git_ref)
        await github.mount_each_file(
            process_file,
            commit,
            github.WalkOptions(
                path_matcher=PatternFilePathMatcher(
                    included_patterns=config.included_patterns,
                    excluded_patterns=config.excluded_patterns,
                ),
            ),
            tenant_key,
            target_table,
        )


@coco.fn
async def process_config_file(
    file: localfs.File,
    target_table: postgres.TableTarget[CodeEmbedding],
) -> None:
    """One config file → many tenants. Re-runs when the file's content
    changes; mounts one ``sync_tenant_repo`` component per tenant key
    under this file's component subpath. Adding or removing a tenant in
    the JSON updates the mount tree on next file change."""
    text = await file.read_text()
    configs = parse_tenant_configs(text)

    async def _mount_tenant(tenant_key: str, config: _RepoConfig) -> None:
        await coco.mount(
            coco.component_subpath(tenant_key),
            coco.auto_refresh(
                sync_tenant_repo, interval=datetime.timedelta(seconds=10)
            ),
            tenant_key,
            config,
            target_table,
        )

    await asyncio.gather(*[_mount_tenant(k, v) for k, v in configs.items()])


@coco.fn
async def app_main(config_dir: pathlib.Path) -> None:
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

    config_files = localfs.walk_dir(
        config_dir,
        recursive=False,
        path_matcher=PatternFilePathMatcher(included_patterns=["*.json"]),
        live=True,
    )
    # NOTE: We don't `mount` for each config file, given we don't treat config files
    # as part of the component path. So that if a tenant's config is moved across config files,
    #
    await coco.map(process_config_file, config_files, target_table)


app = coco.App(
    coco.AppConfig(name="MultiGitHubCodeIndexing"),
    app_main,
    config_dir=pathlib.Path(__file__).parent / "example_configs",
)


# ---------------------------------------------------------------------------
# Query demo
# ---------------------------------------------------------------------------


async def query_once(
    pool: asyncpg.Pool,
    embedder: SentenceTransformerEmbedder,
    query: str,
    *,
    tenant_key: str | None = None,
    top_k: int = TOP_K,
) -> None:
    query_vec = await embedder.embed(query)
    where_clause = "" if tenant_key is None else f"WHERE tenant_key = $3"
    args: list[Any] = [query_vec, top_k]
    if tenant_key is not None:
        args.append(tenant_key)
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT
                tenant_key,
                filename,
                code,
                embedding <=> $1 AS distance,
                start_line,
                end_line
            FROM "{PG_SCHEMA_NAME}"."{TABLE_NAME}"
            {where_clause}
            ORDER BY distance ASC
            LIMIT $2
            """,
            *args,
        )

    for r in rows:
        score = 1.0 - float(r["distance"])
        print(
            f"[{score:.3f}] [{r['tenant_key']}] {r['filename']} "
            f"(L{r['start_line']}-L{r['end_line']})"
        )
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

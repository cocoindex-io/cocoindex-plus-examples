# © 2025 CocoIndex Inc. All rights reserved.
# SPDX-License-Identifier: LicenseRef-CocoIndex-Proprietary
"""
Multi-tenant GitHub Code Indexing (v1) — CocoIndex pipeline example.

Indexes many GitHub repositories at once, one *tenant* per entry in a JSON
config file. The config files themselves live in a GitHub repo too. A
``LiveMap`` decouples the two sides:
  * Producer — ``produce_tenant_configs`` (wrapped in ``coco.auto_refresh``)
    polls the config repo, reads every ``*.json`` file, and declares one map
    entry per tenant (keyed by ``tenant_key``).
  * Consumer — ``coco.mount_each(process_tenant, config_map)`` mounts one
    component per entry, reacting as entries appear, change, and disappear.
Add or remove a tenant key in the config repo and the affected tenants are
added/removed on the next config refresh — no restart.

Compared with v0, v1 doesn't need a separate "meta flow" + custom
``TargetSpec`` to manage tenants. The ``LiveMap`` + component tree give us
that for free: a tenant key is a map entry and a component subpath. Adding a
key in the JSON creates a new entry → a new component; removing a key drops
the entry → the component (and its rows). Changing a key's config re-runs the
tenant with the new parameters.

Two GitHub poll cadences:
  * the config repo — polled by ``produce_tenant_configs`` (tenant membership)
  * each tenant's repo — polled by ``process_tenant`` (code changes)

Indexing (catch-up — one pass, then exit):
    cocoindex update main

Indexing (live — keeps polling the config repo AND each tenant repo):
    cocoindex update -L main

Query:
    python main.py "your query"

Environment:
    GITHUB_APP_ID           — your GitHub App ID
    GITHUB_PRIVATE_KEY_PATH — filesystem path to the App's PEM private key
    POSTGRES_URL            — connection string for the target database
    CONFIG_REPO_OWNER       — owner of the repo holding the tenant config files
                              (default: cocoindex-io)
    CONFIG_REPO_NAME        — name of that config repo
                              (default: cocoindex-plus-examples)
    CONFIG_GIT_REF          — ref to read configs from (default: main)
    CONFIG_DIR              — directory in the config repo holding *.json
                              (default: multi_github_code_indexing/example_configs)

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
from cocoindex.connectors import github, postgres
from cocoindex.ops.text import RecursiveSplitter, detect_code_language
from cocoindex.ops.sentence_transformers import SentenceTransformerEmbedder
from cocoindex.resources.chunk import Chunk
from cocoindex.resources.file import PatternFilePathMatcher
from cocoindex.resources.id import IdGenerator
from cocoindex.resources.live_map import LiveMap
from cocoindex.resources.rate_limit import RateLimiter


DATABASE_URL = os.getenv(
    "POSTGRES_URL", "postgres://cocoindex:cocoindex@localhost/cocoindex"
)
TABLE_NAME = "multi_github_code_indexing"
PG_SCHEMA_NAME = "coco_examples"
TOP_K = 5

# Where the tenant config files live: a directory inside a GitHub repo, read
# through the same GitHub App as the tenant repos.
CONFIG_REPO_OWNER = os.getenv("CONFIG_REPO_OWNER", "cocoindex-io")
CONFIG_REPO_NAME = os.getenv("CONFIG_REPO_NAME", "cocoindex-plus-examples")
CONFIG_GIT_REF = os.getenv("CONFIG_GIT_REF", "main")
CONFIG_DIR = os.getenv("CONFIG_DIR", "multi_github_code_indexing/example_configs")

# Poll cadences. The config repo governs *which* tenants exist; each tenant
# repo governs that tenant's code. They refresh independently.
CONFIG_REFRESH_INTERVAL = datetime.timedelta(seconds=5)
TENANT_REFRESH_INTERVAL = datetime.timedelta(seconds=5)

EMBED_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
PG_DB = coco.ContextKey[asyncpg.Pool]("multi_github_code_embedding_db")
EMBEDDER = coco.ContextKey[SentenceTransformerEmbedder]("embedder", detect_change=True)
GITHUB_APP = coco.ContextKey[github.GitHubApp]("github_app")

_splitter = RecursiveSplitter()


# ---------------------------------------------------------------------------
# Per-tenant config
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _ConfigSource:
    """Where the tenant config files live: a directory inside a GitHub repo."""

    repo_owner: str
    repo_name: str
    git_ref: str
    config_dir: pathlib.PurePosixPath


@dataclass(frozen=True)
class _RepoConfig:
    """One tenant's identity (``tenant_key``) + repo coordinates + filter rules.

    The repo fields are encoded directly in the JSON config file; ``tenant_key``
    is the JSON object key, copied in by ``parse_tenant_configs``. Carrying the
    key in the value lets it ride through the ``LiveMap`` to the consumer, which
    ``coco.mount_each`` hands only the value. ``to_delete: true`` entries are
    skipped in ``parse_tenant_configs`` so the tenant is never declared.
    """

    tenant_key: str
    repo_owner: str
    repo_name: str
    git_ref: str
    included_patterns: list[str] | None = None
    excluded_patterns: list[str] | None = None


def parse_tenant_configs(raw: dict[str, Any]) -> dict[str, _RepoConfig]:
    """Build ``{tenant_key: _RepoConfig}`` from the merged raw JSON config map.

    ``_ConfigCollector`` returns plain JSON (``accept``'s memo only round-trips
    generic types), so the typed parse into ``_RepoConfig`` happens here, after
    the walk. ``to_delete: true`` entries are filtered out so the corresponding
    tenant is never declared — CocoIndex's standard cleanup then removes its
    rows.
    """
    out: dict[str, _RepoConfig] = {}
    for tenant_key, cfg in raw.items():
        if cfg.get("to_delete"):
            continue
        out[tenant_key] = _RepoConfig(
            tenant_key=tenant_key,
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


class _ConfigCollector(github.RepoVisitor[dict[str, Any]]):
    """Merge the raw JSON of every ``*.json`` file in a config subtree up the
    return-value chain, into one ``{tenant_key: raw_config}`` map.

    ``T`` is ``dict[str, Any]`` — plain JSON, **not** ``_RepoConfig``:
    ``File.accept`` / ``Dir.accept`` are memoized and their cache only
    round-trips generic JSON types, so the caller parses the merged result with
    ``parse_tenant_configs`` after the walk. The caller resolves the config
    directory with ``commit.get_object(...)`` and calls ``accept`` on it, so
    this visitor only ever sees that subtree — no pruning needed.

    Config files are deliberately *not* part of the tenant component path:
    tenants are keyed only by ``tenant_key`` (declared by the caller), so moving
    a tenant between config files keeps the same entry (and its rows)."""

    async def visit_directory(
        self, directory: github.Dir, options: github.WalkOptions
    ) -> dict[str, Any]:
        merged: dict[str, Any] = {}
        for child_configs in await self.process_dir_members(directory, options):
            merged.update(child_configs)
        return merged

    async def visit_file(self, file: github.File) -> dict[str, Any]:
        if file.file_path.path.suffix != ".json":
            return {}
        return json.loads(await file.read_text())


@coco.fn
async def produce_tenant_configs(
    config_source: _ConfigSource,
    config_map: LiveMap[str, _RepoConfig],
) -> None:
    """Producer side: one refresh cycle reads every config file from the config
    repo and declares one ``LiveMap`` entry per tenant (keyed by ``tenant_key``).

    Wrapped in ``coco.auto_refresh`` by ``app_main`` so live mode re-polls the
    config repo on a fixed interval. Entries are owned by this component, so a
    tenant dropped from the config (or marked ``to_delete``) stops being
    declared and its entry — and the consumer's component and rows — go away."""
    config_repo = github.GitHubRepo(
        app=coco.use_context(GITHUB_APP),
        owner=config_source.repo_owner,
        repo=config_source.repo_name,
    )
    commit = await config_repo.get_commit(ref=config_source.git_ref)
    config_root = await commit.get_object(config_source.config_dir)
    if config_root is None:
        raise FileNotFoundError(
            f"config dir '{config_source.config_dir}' not found in config repo"
        )
    # Walk just the config subtree via a RepoVisitor; the walk mounts read-only
    # components under "config_files/…" and returns the merged raw JSON, which
    # we then parse into typed _RepoConfig (outside accept's generic-only memo).
    raw_configs = await coco.use_mount(
        coco.component_subpath(coco.Symbol("read_configs")),
        config_root.accept,
        _ConfigCollector(),
        github.WalkOptions(),
    )
    configs = parse_tenant_configs(raw_configs)

    # Declare entries after merging. So in case config files are renamed etc.
    for tenant_key, config in configs.items():
        config_map.declare_entry(tenant_key, config)


@coco.fn
async def process_tenant(
    config: _RepoConfig,
    target_table: postgres.TableTarget[CodeEmbedding],
) -> None:
    """One refresh cycle for one tenant: resolve the configured ref, walk
    the tree, mount each matching file as a per-file processing component.

    Wrapped in ``coco.auto_refresh`` by ``process_tenant`` so live mode polls
    GitHub on a fixed interval. SHA-keyed memoization means unchanged
    blobs are not re-read or re-embedded between cycles.
    """
    gh_repo = github.GitHubRepo(
        app=coco.use_context(GITHUB_APP),
        owner=config.repo_owner,
        repo=config.repo_name,
    )
    with coco.stats_group(f"tenant:{config.tenant_key}", report_to_stdout=True):
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
            config.tenant_key,
            target_table,
        )


@coco.fn
async def app_main(config_source: _ConfigSource) -> None:
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

    # A LiveMap decouples the two sides: the producer polls the config repo and
    # declares one entry per tenant; the consumer mounts one component per entry.
    config_map: LiveMap[str, _RepoConfig] = await LiveMap.create()

    # Producer — GitHub has no filesystem-style watch, so we poll the config
    # repo on a fixed interval instead of `localfs.walk_dir(live=True)`. Await
    # its readiness so the map is populated before the consumer scans it.
    producer = await coco.mount(
        coco.auto_refresh(produce_tenant_configs, interval=CONFIG_REFRESH_INTERVAL),
        config_source,
        config_map,
    )

    # Important: make sure the initial configs are loaded before mounting `process_tenant`.
    # Otherwise `process_tenant` might run on partial or empty config, resulting in targets for
    # certain tenants dropped.
    await producer.ready()

    # Consumer — one tenant component per live-map entry, kept in sync as the
    # producer adds, changes, and removes entries.
    await coco.mount_each(
        coco.auto_refresh(process_tenant, interval=TENANT_REFRESH_INTERVAL),
        config_map,
        target_table,
    )


app = coco.App(
    coco.AppConfig(name="MultiGitHubCodeIndexing"),
    app_main,
    config_source=_ConfigSource(
        repo_owner=CONFIG_REPO_OWNER,
        repo_name=CONFIG_REPO_NAME,
        git_ref=CONFIG_GIT_REF,
        config_dir=pathlib.PurePosixPath(CONFIG_DIR),
    ),
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

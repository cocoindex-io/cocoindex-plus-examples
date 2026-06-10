<!--
© 2025 CocoIndex Inc. All rights reserved.
SPDX-License-Identifier: LicenseRef-CocoIndex-Proprietary
-->

# Multi-tenant GitHub code indexing (v1)

This example indexes many GitHub repositories at once — one *tenant* per entry in a JSON config file — into a single Postgres + pgvector table. The config files themselves live in a GitHub repo: commit a change to `*.json` there and the affected tenants are added or removed on the next config poll, without restarting the app.

## How it works (v1)

The whole pipeline is one CocoIndex `App`. A [`LiveMap`](https://cocoindex.io/docs/common_resources/live_map) decouples the **producer** (reads tenant configs) from the **consumer** (indexes each tenant), so the components stack as two halves joined by the map:

```
app_main
├── LiveMap[tenant_key, _RepoConfig]                       # in-memory shared map
│
├── coco.auto_refresh(produce_tenant_configs, 5min)        # producer: poll config repo
│   └── _ConfigCollector (RepoVisitor)        # walk config repo, read every *.json,
│       │                                     #   merge tenant entries up the return chain
│       └── config_map.declare_entry(tenant_key, config)   # one entry per tenant
│
└── coco.mount_each(process_tenant, config_map)            # consumer: one comp per entry
    └── per tenant_key:
        └── coco.auto_refresh(sync_tenant_repo, 5min)
            └── github.mount_each_file         # walk the configured repo
                └── per file: process_file → process_chunk
```

**Producer.** `produce_tenant_configs` reads the config files with a `github.RepoVisitor` (`_ConfigCollector`): `commit.get_object(config_dir)` resolves the config directory, then `config_root.accept(...)` walks just that subtree — `visit_file` reads one file's raw JSON, `visit_directory` merges its children, and the merged `{tenant_key: raw_config}` map flows up the return-value chain. The visitor returns **raw JSON** rather than typed `_RepoConfig`, because `accept` is memoized and its cache only round-trips generic JSON types; `parse_tenant_configs` does the typed parse afterward. The producer then `declare_entry`s one `LiveMap` entry per tenant. SHA-keyed memoization on `accept` means unchanged config files are not re-read between cycles. Config files are merged by tenant key, so they are deliberately *not* part of the component path — moving a tenant between config files keeps the same entry (and rows).

**Consumer.** `coco.mount_each(process_tenant, config_map)` mounts one component per map entry, keyed by `tenant_key`. CocoIndex tracks ownership of target rows by component path: a new entry creates a component; an entry that disappears (tenant removed from the config, or marked `to_delete: true`) drops the component and CocoIndex deletes its rows automatically. Entries are owned by the producer, so the map — and through it the consumer — follows the config repo with no manual flow management, thread locks, or safeguard timers.

Two GitHub poll cadences, refreshing independently:

- **Config repo** — polled every 5 minutes by `produce_tenant_configs`. Governs *which* tenants exist (the `LiveMap`'s membership).
- **Tenant repos** — polled every 5 minutes per tenant by `sync_tenant_repo` (`mount_each` can't take a live component directly, so `process_tenant` mounts the `auto_refresh` repo walk). Governs each tenant's code. SHA-keyed memoization in `github.File.accept` means unchanged blobs are not re-read or re-embedded between cycles.

## Config file format

The config repo holds one or more `*.json` files (under `CONFIG_DIR`). By default this points at this example's own `example_configs/` on GitHub, so the app runs out of the box; point it at your own repo to manage real tenants. Each file is a map from tenant key to repo config:

```json
{
  "cocoindex_md": {
    "repo_owner": "cocoindex-io",
    "repo_name": "cocoindex",
    "git_ref": "main",
    "included_patterns": ["**/*.md", "**/*.mdx"],
    "excluded_patterns": ["**/.*", "**/target", "**/node_modules"]
  },
  "cocoindex_py": {
    "repo_owner": "cocoindex-io",
    "repo_name": "cocoindex",
    "git_ref": "main",
    "included_patterns": ["python/**/*.py"],
    "excluded_patterns": ["**/.*"]
  },
  "cocoindex_rs": {
    "repo_owner": "cocoindex-io",
    "repo_name": "cocoindex",
    "git_ref": "main",
    "to_delete": true
  }
}
```

`included_patterns` / `excluded_patterns` are passed straight to `PatternFilePathMatcher`. To restrict to a subdirectory, prefix the include pattern (e.g. `python/**/*.py` instead of v0's separate `path` field).

## Prerequisites

- [Install Postgres](https://cocoindex.io/docs/getting_started/installation#-install-postgres) with the `vector` extension.
- A GitHub App with read access to **both** the config repo and the tenant repos you want to index. Save its App ID and the PEM private key path.
- A GitHub repo holding your `*.json` tenant config files. The defaults point at this example's own [`multi_github_code_indexing/example_configs`](https://github.com/cocoindex-io/cocoindex-plus-examples/tree/main/multi_github_code_indexing/example_configs); set `CONFIG_REPO_OWNER`/`CONFIG_REPO_NAME`/`CONFIG_DIR` to use your own.

## Setup

Copy `.env.example` to `.env` and fill in:

```
POSTGRES_URL=postgres://...
GITHUB_APP_ID=...
GITHUB_PRIVATE_KEY_PATH=/path/to/key.pem
CONFIG_REPO_OWNER=cocoindex-io                          # defaults to this example's repo
CONFIG_REPO_NAME=cocoindex-plus-examples
CONFIG_GIT_REF=main                                     # optional, defaults to main
CONFIG_DIR=multi_github_code_indexing/example_configs   # optional, dir holding *.json
```

Install:

```bash
pip install -e .
```

## Run

Catch-up mode — one pass, then exit:

```bash
cocoindex update main
```

Live mode — keeps the app running, polls the config repo for tenant membership changes and each tenant's GitHub ref for code changes, both every 5 minutes:

```bash
cocoindex update -L main
```

Query (per-tenant filter optional via the SQL `WHERE tenant_key = …` clause shown in `main.py`):

```bash
python main.py "your search query"
```

## Notes

- **Rate limiting**: v1's GitHub connector doesn't yet have an in-process throttle. Several tenants walking the same App in parallel can briefly burst against the GitHub API; the 429-retry loop will recover. If you have many tenants, consider staggering `auto_refresh` intervals or running fewer concurrent tenants.
- **Schema changes**: the `code_embeddings` table now has a `tenant_key` column. If you ran a previous version of this example, drop the table before re-indexing.

## CocoInsight

Optional UI for inspecting the pipeline:

```bash
cocoindex server -ci main.py
```

Then open [https://cocoindex.io/cocoinsight](https://cocoindex.io/cocoinsight).

<!--
© 2025 CocoIndex Inc. All rights reserved.
SPDX-License-Identifier: LicenseRef-CocoIndex-Proprietary
-->

# Multi-tenant GitHub code indexing (v1)

This example indexes many GitHub repositories at once — one *tenant* per entry in a JSON config file — into a single Postgres + pgvector table. Tenant configs are picked up live: edit `example_configs/*.json` and the affected tenants are added or removed without restarting the app.

## How it works (v1)

The whole pipeline is one CocoIndex `App`. Components stack as a tree:

```
app_main
└── localfs.walk_dir(config_dir, live=True)      # watch JSON config files
    └── process_config_file                      # parse JSON → many tenants
        └── per tenant_key:
            └── coco.auto_refresh(sync_tenant_repo, interval=5min)
                └── github.mount_each_file       # walk the configured repo
                    └── per file: process_file → process_chunk
```

Each tenant key becomes a component subpath; CocoIndex tracks ownership of target rows by component path. Adding a key in the JSON creates a new component; removing a key (or marking it `to_delete: true`) drops the component and CocoIndex deletes its rows automatically. No manual flow management, no thread locks, no safeguard timers.

Two refresh cadences:

- **Config files** — watched live via `localfs.walk_dir(live=True)`. Edits propagate within seconds.
- **GitHub commits** — polled every 5 minutes per tenant by `coco.auto_refresh`. SHA-keyed memoization in `github.File.accept` means unchanged blobs are not re-read or re-embedded between cycles.

## Config file format

`example_configs/*.json` contains a map from tenant key to repo config:

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
- A GitHub App with read access to the repos you want to index. Save its App ID and the PEM private key path.

## Setup

Copy `.env.example` to `.env` and fill in:

```
POSTGRES_URL=postgres://...
GITHUB_APP_ID=...
GITHUB_PRIVATE_KEY_PATH=/path/to/key.pem
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

Live mode — keeps the app running, watches `example_configs/` for config changes, polls each tenant's GitHub ref every 5 minutes:

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

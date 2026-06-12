<!--
© 2025 CocoIndex Inc. All rights reserved.
SPDX-License-Identifier: LicenseRef-CocoIndex-Proprietary
-->

# Code Elements Indexing (v1)

This example uses CocoIndex to extract structural code elements from a GitHub repository — classes, functions, methods, and the call / type references between them — and index them into two PostgreSQL tables.

## What it does

For each Python (`.py`) and C# (`.cs`) file in the repository, the pipeline:

1. Detects the programming language from the filename.
2. Runs `ExtractCodeElements` (TreeSitter-based AST analysis) to find all structural **declarations** and all call / type **references**.
3. Writes one row per declaration to `code_declarations` and one row per reference to `code_references`.

GitHub refs are polled every 10 seconds in live mode; SHA-keyed memoization in the GitHub source means unchanged blobs are not re-read or re-parsed between cycles.

## Prerequisites

- [Install Postgres](https://cocoindex.io/docs/getting_started/installation#-install-postgres).
- A GitHub App with read access to the target repository (App ID + PEM private key).
- [uv](https://docs.astral.sh/uv/) (recommended) or pip.

## Setup

Copy `.env.example` to `.env` and fill in:

```
POSTGRES_URL=postgres://user:password@localhost/dbname
GITHUB_APP_ID=<your-github-app-id>
GITHUB_PRIVATE_KEY_PATH=<path-to-your-github-app-private-key.pem>
GITHUB_REPO_OWNER=<owner>     # defaults to cocoindex-io
GITHUB_REPO_NAME=<repo>       # defaults to cocoindex
GITHUB_REPO_REF=<branch>      # defaults to main
```

Install:

```bash
uv sync
```

## Run

Catch-up mode — one pass, then exit:

```bash
cocoindex update main
```

Live mode — keeps the app running and re-polls the configured ref every 10 seconds:

```bash
cocoindex update -L main
```

## Output tables (schema: `coco_examples`)

### `code_declarations`

| Column | Type | Description |
|--------|------|-------------|
| `id` | uuid | Stable per-declaration UUID |
| `filename` | text | Source file path |
| `language` | text | `python` or `csharp` |
| `namespace` | text | Namespace or module path |
| `entity_name` | text | Fully qualified entity name (e.g. `OrderService.PlaceOrder`) |
| `parent_entity_name` | text | Enclosing entity name, or null if top-level |
| `base_name` | text | Simple (unqualified) name |
| `kind` | text | Normalized cross-language kind (e.g. `class`, `method`, `type_alias`) |
| `ast_node_kind` | text | TreeSitter node kind (e.g. `class_declaration`, `function_definition`) |
| `has_body` | bool | Whether the declaration has a body |
| `start_line` / `start_column` / `start_char_offset` | int | Start position |
| `end_line` / `end_column` / `end_char_offset` | int | End position |

### `code_references`

| Column | Type | Description |
|--------|------|-------------|
| `id` | uuid | Stable per-reference UUID |
| `filename` | text | Source file path |
| `language` | text | Programming language |
| `namespace` | text | Namespace at the call site |
| `parent_entity_name` | text | Enclosing entity name, or null if at module level |
| `referenced_base_name` | text | Simple name of the referenced entity |
| `referenced_full_path` | text | Full dotted path (e.g. `helper.Process`) |
| `ast_node_kind` | text | TreeSitter node kind (e.g. `invocation_expression`, `call`) |
| `start_line` / `start_column` / `start_char_offset` | int | Start position |
| `end_line` / `end_column` / `end_char_offset` | int | End position |

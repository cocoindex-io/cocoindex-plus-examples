# Code Elements Indexing

This example uses CocoIndex to extract code declarations and references from a GitHub repository and index them into two PostgreSQL tables using the `ExtractCodeElements` function.

## What it does

For each Python (`.py`) and C# (`.cs`) file in the repository, the flow:

1. Detects the programming language from the filename.
2. Extracts all structural **declarations** (classes, functions, methods) and **references** (call sites, object creation) using TreeSitter AST analysis.
3. Stores the results in two tables:
   - `code_declarations` — one row per declaration, with namespace, entity name, parent entity, body presence, and source position.
   - `code_references` — one row per call/instantiation site, with namespace, parent entity, referenced path, and source position.

## Setup

### Prerequisites

- PostgreSQL database
- GitHub App with read access to the target repository
- [uv](https://docs.astral.sh/uv/) package manager

### Configuration

Copy `.env.example` to `.env` and fill in:

```
COCOINDEX_DATABASE_URL=postgres://user:password@localhost/dbname
GITHUB_APP_ID=<your-github-app-id>
GITHUB_PRIVATE_KEY_PATH=<path-to-your-github-app-private-key.pem>
```

Update the `owner`, `repo`, and `git_ref` fields in `main.py` to point to your target repository.

### Install dependencies

```bash
uv sync
```

### Run

```bash
uv run python main.py
```

This will run the indexing flow and print update statistics when done.

## Output tables

### `code_declarations`

| Column | Type | Description |
|--------|------|-------------|
| `filename` | text | Source file path |
| `language` | text | Programming language (`python` or `csharp`) |
| `namespace` | text | Namespace or module path |
| `entity_name` | text | Fully qualified entity name (e.g. `OrderService.PlaceOrder`) |
| `parent_entity_name` | text | Enclosing entity name, or null if top-level |
| `base_name` | text | Simple (unqualified) name |
| `ast_node_kind` | text | TreeSitter node kind (e.g. `class_declaration`, `function_definition`) |
| `has_body` | bool | Whether the declaration has a body |
| `start` | jsonb | Start position: `{offset, line, column}` |
| `end` | jsonb | End position: `{offset, line, column}` |

### `code_references`

| Column | Type | Description |
|--------|------|-------------|
| `filename` | text | Source file path |
| `language` | text | Programming language |
| `namespace` | text | Namespace at the call site |
| `parent_entity_name` | text | Enclosing entity name, or null if at module level |
| `referenced_base_name` | text | Simple name of the referenced entity |
| `referenced_full_path` | text | Full dotted path (e.g. `helper.Process`) |
| `ast_node_kind` | text | TreeSitter node kind (e.g. `invocation_expression`, `call`) |
| `start` | jsonb | Start position: `{offset, line, column}` |
| `end` | jsonb | End position: `{offset, line, column}` |

<!--
© 2025 CocoIndex Inc. All rights reserved.
SPDX-License-Identifier: LicenseRef-CocoIndex-Proprietary
-->

# Build index for GitHub repository

This example demonstrates how to build an index for a GitHub repository using CocoIndex.

## Steps

### Indexing Flow

1. We will ingest a GitHub repository.
2. For each file, perform chunking (Tree-sitter) and then embedding.
3. We will save the embeddings and the metadata in Postgres with PGVector.
4. Create a `.env` file from `.env.example`, and fill configurations for your GitHub app.

**Note:** You need to configure the GitHub source with your repository details:
- `repo_name`: The GitHub repository name (e.g., "owner/repo-name")
- `branch`: The branch to index (e.g., "main")
- `private_key_path`: Path to your private key for authentication

### Query:
We will match against user-provided text by a SQL query, reusing the embedding operation in the indexing flow.


## Prerequisite
[Install Postgres](https://cocoindex.io/docs/getting_started/installation#-install-postgres) if you don't have one.

## Run

- Install dependencies:
  ```bash
  pip install -e .
  ```

- Setup:

  ```bash
  cocoindex setup main.py
  ```

- Update index:

  ```bash
  cocoindex update main.py
  ```

- Run:

  ```bash
  python main.py
  ```


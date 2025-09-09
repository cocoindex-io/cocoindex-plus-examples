<!--
© 2025 CocoIndex Inc. All rights reserved.
SPDX-License-Identifier: LicenseRef-CocoIndex-Proprietary
-->

# Build index for multiple GitHub repositories (meta flow + code indexing flows)

## Flows

This example demonstrates how to build an index for multiple GitHub repositories using CocoIndex.

- We use a meta flow to read a config file containing multiple GitHub repositories and derive a dedicated code indexing flow for each.
- Each code indexing flow is instantiated based on the repository config.

### Meta Flow

This is what the meta flow does:

1. Ingest a config file containing multiple GitHub repositories.
2. Parse the config file to extract the repository config.
3. Export the repository config to a custom target that captures events of adding, updating, or deleting a repository config.

The custom target maintains a sets of `cocoindex.Flow` instances, one for each repository config entry.


### Code Indexing Flow

This is what the code indexing flow does:

1. Ingest a GitHub repository.
   Specific configs for the GitHub repository are parameters passed from the meta flow.
2. For each file, perform chunking (Tree-sitter) and then embedding.
3. We will save the embeddings and the metadata in Postgres with PGVector.
4. Create a `.env` file from `.env.example`, and fill configurations for your GitHub app.


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

- Run (which will continuously run and update the index):

  ```bash
  python main.py
  ```

## CocoInsight
I used CocoInsight (Free beta now) to troubleshoot the index generation and understand the data lineage of the pipeline.
It just connects to your local CocoIndex server, with Zero pipeline data retention. Run the following command to start CocoInsight:

```
cocoindex server -ci --reexport main.py
```

The meta flow needs to load `cocoindex.Flow` instances in memory, so we need to use the `--reexport` option to reexport the targets each time the meta flow reloads.

Then open the CocoInsight UI at [https://cocoindex.io/cocoinsight](https://cocoindex.io/cocoinsight).

<img width="1305" alt="Chunking Visualization" src="https://github.com/user-attachments/assets/8e83b9a4-2bed-456b-83e5-b5381b28b84a" />

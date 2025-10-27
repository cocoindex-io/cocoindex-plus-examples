# © 2025 CocoIndex Inc. All rights reserved.
# SPDX-License-Identifier: LicenseRef-CocoIndex-Proprietary

from dataclasses import dataclass
import datetime
from threading import Lock
import json
import functools
import dotenv
from psycopg_pool import ConnectionPool
from pgvector.psycopg import register_vector

import cocoindex
import os
from numpy.typing import NDArray
import numpy as np

# Add a safeguard period for now. A flow, if closing happens more than this
# duration after it is opened, avoid closing it.
# This is because currently configs from different source files are treated as
# different source rows, so a upsert + a delete are triggered in parallel for
# the specific config item. This adds a safeguard period to avoid closing the
# flow immediately after it is opened in this case.
# After we have official source-level multi-tenant support, we won't need this.
FLOW_OPEN_SAFEGUARD_DURATION = datetime.timedelta(seconds=5)


# Config for a GitHub repo to be indexed.
@dataclass
class _GitHubRepoConfig:
    repo_owner: str
    repo_name: str
    git_ref: str

    path: str | None = None
    included_patterns: list[str] | None = None
    excluded_patterns: list[str] | None = None

    to_delete: bool = False


def _get_github_app_spec() -> cocoindex.sources.GitHubApp:
    return cocoindex.sources.GitHubApp(
        app_id=int(os.environ["GITHUB_APP_ID"]),
        private_key_path=os.environ["GITHUB_PRIVATE_KEY_PATH"],
        # Global rate limit shared by all sources using the same app.
        rate_limit=cocoindex.RateLimit(max_rows_per_second=1),
    )


####################################################################################################
# Code to define meta flow: the CocoIndex flow that manages code indexing flows for multiple repos.
####################################################################################################


@dataclass
class _GitHubRepoConfigRow:
    config: _GitHubRepoConfig


# Manager for all code indexing flows.
class _CodeIndexingFlows:
    _flow_name_prefix: str
    _flows_lock: Lock
    _flows: dict[
        str, tuple[cocoindex.Flow, cocoindex.FlowLiveUpdater, datetime.datetime]
    ]

    def __init__(self, flow_name_prefix: str):
        self._flow_name_prefix = flow_name_prefix
        self._flows_lock = Lock()
        self._flows = {}

    def upsert_flow(self, key: str, repo_config: _GitHubRepoConfig) -> None:
        with self._flows_lock:
            if key in self._flows:
                existing_flow, updater, _ = self._flows[key]
                updater.abort()
                updater.wait()
                existing_flow.close()
                del self._flows[key]

            flow = _build_code_indexing_flow(
                f"{self._flow_name_prefix}_{key}", repo_config
            )
            try:
                flow.setup(report_to_stdout=True)
                updater = cocoindex.FlowLiveUpdater(
                    flow, cocoindex.FlowLiveUpdaterOptions(print_stats=True)
                )
                updater.start()
            except Exception as e:
                flow.close()
                raise e
            self._flows[key] = (flow, updater, datetime.datetime.now())

    def delete_flow(self, key: str) -> None:
        with self._flows_lock:
            # Known limitation: if the flow is not loaded in memory, we cannot drop it.
            # This is because currently dropping a flow requires analyzing the flow first.
            # We will lift this limitation and allow to directly drop the flow by name later.

            if key in self._flows:
                flow, updater, _ = self._flows[key]
                updater.abort()
                updater.wait()
                flow.drop(report_to_stdout=True)

    def close_flow(self, key: str) -> None:
        with self._flows_lock:
            if key in self._flows:
                flow, updater, open_time = self._flows[key]
                open_duration = datetime.datetime.now() - open_time
                if open_duration > FLOW_OPEN_SAFEGUARD_DURATION:
                    print(f"Closing flow {key}.")
                    updater.abort()
                    updater.wait()
                    flow.close()
                    del self._flows[key]
                else:
                    print(
                        f"Skipping closing flow {key} because it has been open for {open_duration} < {FLOW_OPEN_SAFEGUARD_DURATION}."
                    )


# _CodeIndexingFlowsManager is a CocoIndex target that synchronizes all repo configs with
# _CodeIndexingFlows.
class _CodeIndexingFlowsManager(cocoindex.op.TargetSpec):
    indexing_flow_name_prefix: str


@cocoindex.op.target_connector(spec_cls=_CodeIndexingFlowsManager)
class _CodeIndexingFlowsManagerConnector:
    @staticmethod
    def get_persistent_key(spec: _CodeIndexingFlowsManager, target_name: str) -> str:
        return spec.indexing_flow_name_prefix

    @staticmethod
    def apply_setup_change(
        key: str,
        previous: _CodeIndexingFlowsManager | None,
        current: _CodeIndexingFlowsManager | None,
    ) -> None:
        pass

    @staticmethod
    def prepare(spec: _CodeIndexingFlowsManager) -> _CodeIndexingFlows:
        return _CodeIndexingFlows(spec.indexing_flow_name_prefix)

    @staticmethod
    def mutate(
        *all_mutations: tuple[
            _CodeIndexingFlows, dict[str, _GitHubRepoConfigRow | None]
        ],
    ) -> None:
        for flows, mutations in all_mutations:
            for key, mutation in mutations.items():
                if mutation is None:
                    flows.close_flow(key)
                elif mutation.config.to_delete:
                    flows.delete_flow(key)
                else:
                    flows.upsert_flow(key, mutation.config)


# Helper function to parse the repo config file.
@cocoindex.op.function()
def parse_repo_config(repo_config: str) -> dict[str, _GitHubRepoConfigRow]:
    configs = json.loads(repo_config)
    return {
        key: _GitHubRepoConfigRow(config=_GitHubRepoConfig(**config))
        for key, config in configs.items()
    }


# Now define the meta flow that manages all code indexing flows.
@cocoindex.flow_def(name="MultiGithubCodeIndexing")
def meta_flow(
    flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope
) -> None:
    data_scope["config_files"] = flow_builder.add_source(
        cocoindex.sources.LocalFile(
            path="example_configs", included_patterns=["*.json"]
        ),
        # Using LocalFile for simplicity, but you can also switch to GitHub source to read configs
        # from a GitHub repo, e.g.,
        #
        #   cocoindex.sources.GitHub(
        #       app=_get_github_app_spec(),
        #       owner="cocoindex-io",
        #       repo="cocoindex-plus",
        #       git_ref="main",
        #       path="examples/multi_github_code_indexing/example_configs",
        #       included_patterns=["*.json"],
        #   ),
        refresh_interval=datetime.timedelta(seconds=10),
    )
    repo_configs = data_scope.add_collector()
    with data_scope["config_files"].row() as config_file:
        config_file["configs"] = config_file["content"].transform(parse_repo_config)
        with config_file["configs"].row() as config_item:
            repo_configs.collect(
                key=config_item[cocoindex.typing.KEY_FIELD_NAME],
                config=config_item["config"],
            )

    repo_configs.export(
        "flows_manager",
        _CodeIndexingFlowsManager(
            indexing_flow_name_prefix="multi_github_indexing",
        ),
        primary_key_fields=["key"],
    )


####################################################################################################
# Code to define code indexing flows.
####################################################################################################


@cocoindex.op.function()
def extract_extension(filename: str) -> str:
    """Extract the extension of a filename."""
    return os.path.splitext(filename)[1]


@cocoindex.transform_flow()
def code_to_embedding(
    text: cocoindex.DataSlice[str],
) -> cocoindex.DataSlice[NDArray[np.float32]]:
    """
    Embed the text using a SentenceTransformer model.
    """
    # You can also switch to Voyage embedding model:
    #    return text.transform(
    #        cocoindex.functions.EmbedText(
    #            api_type=cocoindex.LlmApiType.VOYAGE,
    #            model="voyage-code-3",
    #        )
    #    )
    return text.transform(
        cocoindex.functions.SentenceTransformerEmbed(
            model="sentence-transformers/all-MiniLM-L6-v2"
        )
    )


@functools.cache
def connection_pool() -> ConnectionPool:
    """
    Get a connection pool to the database.
    """
    return ConnectionPool(os.environ["COCOINDEX_DATABASE_URL"])


TOP_K = 5


# This is the factory function that builds the code indexing flow for a given repo info.
def _build_code_indexing_flow(
    flow_name: str, repo_config: _GitHubRepoConfig
) -> cocoindex.Flow:
    """
    Define an example flow that embeds files into a vector database.
    """

    @cocoindex.flow_def(name=flow_name)
    def flow(
        flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope
    ) -> None:
        data_scope["files"] = flow_builder.add_source(
            cocoindex.sources.GitHub(
                app=_get_github_app_spec(),
                owner=repo_config.repo_owner,
                repo=repo_config.repo_name,
                git_ref=repo_config.git_ref,
                path=repo_config.path,
                included_patterns=repo_config.included_patterns,
                excluded_patterns=repo_config.excluded_patterns,
            ),
            refresh_interval=datetime.timedelta(seconds=60),
        )
        code_embeddings = data_scope.add_collector()

        with data_scope["files"].row() as file:
            file["extension"] = file["filename"].transform(extract_extension)
            file["chunks"] = file["content"].transform(
                cocoindex.functions.SplitRecursively(),
                language=file["extension"],
                chunk_size=1000,
                min_chunk_size=300,
                chunk_overlap=300,
            )
            with file["chunks"].row() as chunk:
                chunk["embedding"] = chunk["text"].call(code_to_embedding)
                code_embeddings.collect(
                    filename=file["filename"],
                    location=chunk["location"],
                    code=chunk["text"],
                    embedding=chunk["embedding"],
                    start=chunk["start"],
                    end=chunk["end"],
                )

        code_embeddings.export(
            "code_embeddings",
            cocoindex.targets.Postgres(),
            primary_key_fields=["filename", "location"],
            vector_indexes=[
                cocoindex.VectorIndexDef(
                    field_name="embedding",
                    metric=cocoindex.VectorSimilarityMetric.COSINE_SIMILARITY,
                )
            ],
        )

    @flow.query_handler(
        name="search",
        result_fields=cocoindex.QueryHandlerResultFields(
            embedding=["embedding"], score="score"
        ),
    )
    def search(query: str) -> None:
        # Get the table name, for the export target in the github_code_indexing_flow above.
        table_name = cocoindex.utils.get_target_default_name(flow, "code_embeddings")
        # Evaluate the transform flow defined above with the input query, to get the embedding.
        query_vector = code_to_embedding.eval(query)
        # Run the query and get the results.
        with connection_pool().connection() as conn:
            register_vector(conn)
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT filename, code, embedding, embedding <=> %s AS distance, start, "end"
                    FROM {table_name} ORDER BY distance LIMIT %s
                """,
                    (query_vector, TOP_K),
                )
                return cocoindex.QueryOutput(
                    query_info=cocoindex.QueryInfo(
                        embedding=query_vector,
                        similarity_metric=cocoindex.VectorSimilarityMetric.COSINE_SIMILARITY,
                    ),
                    results=[
                        {
                            "filename": row[0],
                            "code": row[1],
                            "embedding": row[2],
                            "score": 1.0 - row[3],
                            "start": row[4],
                            "end": row[5],
                        }
                        for row in cur.fetchall()
                    ],
                )

    return flow


if __name__ == "__main__":
    dotenv.load_dotenv()
    cocoindex.init()
    options = cocoindex.FlowLiveUpdaterOptions(print_stats=True, reexport_targets=True)
    with cocoindex.FlowLiveUpdater(meta_flow, options) as updater:
        updater.wait()

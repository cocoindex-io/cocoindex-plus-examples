# © 2025 CocoIndex Inc. All rights reserved.
# SPDX-License-Identifier: LicenseRef-CocoIndex-Proprietary

from dotenv import load_dotenv
from psycopg_pool import ConnectionPool
from pgvector.psycopg import register_vector
import functools
import cocoindex
import os
from numpy.typing import NDArray
import numpy as np

import cocoindex.functions.chonkie as coco_chonkie


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


@cocoindex.flow_def(name="GithubCodeIndexing")
def github_code_indexing_flow(
    flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope
) -> None:
    """
    Define an example flow that embeds files into a vector database.
    """
    data_scope["files"] = flow_builder.add_source(
        cocoindex.sources.GitHub(
            app=cocoindex.sources.GitHubApp(
                app_id=int(os.environ["GITHUB_APP_ID"]),
                private_key_path=os.environ["GITHUB_PRIVATE_KEY_PATH"],
            ),
            owner="cocoindex-io",
            repo="cocoindex",
            # owner="georgeh0",
            # repo="llvm-project",
            # path="clang",
            git_ref="main",
            included_patterns=[
                "*.py",
                "*.h",
                "*.cpp",
                "*.c",
                "*.rs",
                "*.toml",
                "*.md",
                "*.mdx",
            ],
            excluded_patterns=["**/.*", "target", "**/node_modules"],
            # Optional, can be omitted if using "https://api.github.com"
            api_base_url="https://api.github.com",
        ),
        rate_limit=cocoindex.RateLimit(max_rows_per_second=1),
    )
    code_embeddings = data_scope.add_collector()

    with data_scope["files"].row() as file:
        file["language"] = file["filename"].transform(
            cocoindex.functions.DetectProgrammingLanguage()
        )

        # Use SplitRecursively
        # file["chunks"] = file["content"].transform(
        #     cocoindex.functions.SplitRecursively(),
        #     language=file["language"],
        #     chunk_size=1000,
        #     min_chunk_size=300,
        #     chunk_overlap=300,
        # )

        # Use ChonkieRecursiveChunker
        #   file["chunks"] = file["content"].transform(
        #       coco_chonkie.ChonkieRecursiveChunker(
        #           chunk_size=1000,
        #       )
        #   )

        # Use ChonkieCodeChunker
        file["chunks"] = file["content"].transform(
            coco_chonkie.ChonkieCodeChunker(chunk_size=1000),
            language=file["language"],
        )

        # Use ChonkieSemanticChunker
        #   file["chunks"] = file["content"].transform(
        #       coco_chonkie.ChonkieSemanticChunker(
        #           chunk_size=1000,
        #       )
        #   )

        # Use ChonkieNeuralChunker
        #    file["chunks"] = file["content"].transform(
        #        coco_chonkie.ChonkieNeuralChunker(device_map="mps"),
        #    )

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
        cocoindex.targets.Postgres(
            column_options={
                "embedding": cocoindex.targets.PostgresColumnOptions(type="halfvec"),
            }
        ),
        primary_key_fields=["filename", "location"],
        vector_indexes=[
            cocoindex.VectorIndexDef(
                field_name="embedding",
                metric=cocoindex.VectorSimilarityMetric.COSINE_SIMILARITY,
            )
        ],
    )


@functools.cache
def connection_pool() -> ConnectionPool:
    """
    Get a connection pool to the database.
    """
    return ConnectionPool(os.environ["COCOINDEX_DATABASE_URL"])


TOP_K = 5


# Declaring it as a query handler, so that you can easily run queries in CocoInsight.
@github_code_indexing_flow.query_handler(
    result_fields=cocoindex.QueryHandlerResultFields(
        embedding=["embedding"], score="score"
    )
)
def search(query: str) -> cocoindex.QueryOutput:
    # Get the table name, for the export target in the github_code_indexing_flow above.
    table_name = cocoindex.utils.get_target_default_name(
        github_code_indexing_flow, "code_embeddings"
    )
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


def _main() -> None:
    # Make sure the flow is built and up-to-date.
    stats = github_code_indexing_flow.update()
    print("Updated index: ", stats)

    # Run queries in a loop to demonstrate the query capabilities.
    while True:
        query = input("Enter search query (or Enter to quit): ")
        if query == "":
            break
        # Run the query function with the database connection pool and the query.
        query_output = search(query)
        print("\nSearch results:")
        for result in query_output.results:
            print(
                f"[{result['score']:.3f}] {result['filename']} (L{result['start']['line']}-L{result['end']['line']})"
            )
            print(f"    {result['code']}")
            print("---")
        print()


if __name__ == "__main__":
    load_dotenv()
    cocoindex.init()
    _main()

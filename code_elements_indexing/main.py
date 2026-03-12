# © 2025 CocoIndex Inc. All rights reserved.
# SPDX-License-Identifier: LicenseRef-CocoIndex-Proprietary

from dotenv import load_dotenv
import cocoindex
import os


_CODE_ELEMENTS_LANGUAGE_CONFIG: dict[
    str, cocoindex.functions.CodeElementsLanguageConfig
] = {
    "python": cocoindex.functions.CodeElementsLanguageConfig(
        declaration_node_kinds={
            "class_definition": cocoindex.functions.CodeElementsDeclarationConfig(
                name_field="name", body_field="body"
            ),
            "function_definition": cocoindex.functions.CodeElementsDeclarationConfig(
                name_field="name", body_field="body"
            ),
        },
        reference_node_kinds={
            "call": cocoindex.functions.CodeElementsReferenceConfig(
                path_expr_field="function"
            ),
            "typed_parameter": cocoindex.functions.CodeElementsReferenceConfig(
                path_expr_field="type"
            ),
            "typed_default_parameter": cocoindex.functions.CodeElementsReferenceConfig(
                path_expr_field="type"
            ),
        },
    ),
    "csharp": cocoindex.functions.CodeElementsLanguageConfig(
        declaration_node_kinds={
            "class_declaration": cocoindex.functions.CodeElementsDeclarationConfig(
                name_field="name", body_field="body"
            ),
            "struct_declaration": cocoindex.functions.CodeElementsDeclarationConfig(
                name_field="name", body_field="body"
            ),
            "interface_declaration": cocoindex.functions.CodeElementsDeclarationConfig(
                name_field="name", body_field="body"
            ),
            "enum_declaration": cocoindex.functions.CodeElementsDeclarationConfig(
                name_field="name", body_field="body"
            ),
            "record_declaration": cocoindex.functions.CodeElementsDeclarationConfig(
                name_field="name", body_field="body"
            ),
            "method_declaration": cocoindex.functions.CodeElementsDeclarationConfig(
                name_field="name", body_field="body"
            ),
            "constructor_declaration": cocoindex.functions.CodeElementsDeclarationConfig(
                name_field="name", body_field="body"
            ),
        },
        reference_node_kinds={
            "invocation_expression": cocoindex.functions.CodeElementsReferenceConfig(
                path_expr_field="function"
            ),
            "object_creation_expression": cocoindex.functions.CodeElementsReferenceConfig(
                path_expr_field="type"
            ),
            "parameter": cocoindex.functions.CodeElementsReferenceConfig(
                path_expr_field="type"
            ),
        },
        type_list_node_kinds={
            "base_list": cocoindex.functions.CodeElementsTypeListConfig(),
            "type_argument_list": cocoindex.functions.CodeElementsTypeListConfig(),
        },
        namespace_node_kinds={
            "namespace_declaration": cocoindex.functions.CodeElementsNamespaceConfig(
                name_field="name"
            ),
        },
    ),
}


@cocoindex.flow_def(name="CodeElementsIndexing")
def code_elements_flow(
    flow_builder: cocoindex.FlowBuilder, data_scope: cocoindex.DataScope
) -> None:
    """
    Define a flow that extracts code declarations and references from a GitHub
    repository and indexes them into two PostgreSQL tables.
    """
    data_scope["files"] = flow_builder.add_source(
        cocoindex.sources.GitHub(
            app=cocoindex.sources.GitHubApp(
                app_id=int(os.environ["GITHUB_APP_ID"]),
                private_key_path=os.environ["GITHUB_PRIVATE_KEY_PATH"],
            ),
            owner=os.environ.get("GITHUB_REPO_OWNER", "cocoindex-io"),
            repo=os.environ.get("GITHUB_REPO_NAME", "cocoindex"),
            git_ref=os.environ.get("GITHUB_REPO_REF", "main"),
            included_patterns=["*.cs", "*.py"],
            excluded_patterns=["**/.*"],
        ),
        rate_limit=cocoindex.RateLimit(max_rows_per_second=10),
    )
    declarations = data_scope.add_collector()
    references = data_scope.add_collector()

    with data_scope["files"].row() as file:
        file["language"] = file["filename"].transform(
            cocoindex.functions.DetectProgrammingLanguage()
        )
        file["elements"] = file["content"].transform(
            cocoindex.functions.ExtractCodeElements(
                languages=_CODE_ELEMENTS_LANGUAGE_CONFIG
            ),
            language=file["language"],
        )

        with file["elements"]["declarations"].row() as decl:
            declarations.collect(
                id=cocoindex.GeneratedField.UUID,
                filename=file["filename"],
                language=file["language"],
                namespace=decl["namespace"],
                entity_name=decl["entity_name"],
                parent_entity_name=decl["parent_entity_name"],
                base_name=decl["base_name"],
                ast_node_kind=decl["ast_node_kind"],
                has_body=decl["has_body"],
                start=decl["start"],
                end=decl["end"],
            )

        with file["elements"]["references"].row() as ref:
            references.collect(
                id=cocoindex.GeneratedField.UUID,
                filename=file["filename"],
                language=file["language"],
                namespace=ref["namespace"],
                parent_entity_name=ref["parent_entity_name"],
                referenced_base_name=ref["referenced_base_name"],
                referenced_full_path=ref["referenced_full_path"],
                ast_node_kind=ref["ast_node_kind"],
                start=ref["start"],
                end=ref["end"],
            )

    declarations.export(
        "code_declarations",
        cocoindex.targets.Postgres(),
        primary_key_fields=["id"],
    )
    references.export(
        "code_references",
        cocoindex.targets.Postgres(),
        primary_key_fields=["id"],
    )


def _main() -> None:
    stats = code_elements_flow.update()
    print("Updated index:", stats)


if __name__ == "__main__":
    load_dotenv()
    cocoindex.init()
    _main()

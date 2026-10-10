<!--
© 2026 CocoIndex Inc. All rights reserved.
SPDX-License-Identifier: LicenseRef-CocoIndex-Proprietary
-->

# Many sources, one pipeline, one knowledge graph

**A Postgres catalog, a folder of Markdown docs, and the issues, pull requests, and releases of a GitHub repo land in one Neo4j graph.** Every source is a small adapter; everything after it is shared — in plain async Python. Nodes that several sources know about, the entities and the people, are [shared components](https://cocoindex.io/docs/programming_guide/processing_component#shared-components-plus): held by every source that mentions them, gone when the last one lets go.

Knowledge about a software system is spread over dozens of systems: a catalog says what exists, docs explain it, the issue tracker records what is broken, pull requests and release notes record what changed. The processing is mostly the same for all of them — find which entities a piece of text is about, link it to them — and only the shape of each source differs. This example is built in that shape. A **source** is a name plus an async generator of refs; a **document** from any source goes through the same `add_document`; and the result is **one** Neo4j graph where a `Document` is a docs page, an issue, a pull request, or a release, any of them can `MENTIONS` any `Entity`, and a `Person` `AUTHORED` issues, pull requests, and releases alike.

By default the catalog describes CocoIndex itself and the docs, issues, pull requests, and releases are CocoIndex's own, so you get an engineering knowledge graph of this repository. Point the settings at your own catalog, docs, and GitHub or GitHub Enterprise repo instead. No LLM, no API keys; the only services are Postgres and Neo4j.

## How it works

The pipeline has a per-source front half and a shared back half, and the files follow the split:

| Phase | Shared? | Where |
|---|---|---|
| List refs, fetch one, extract into a record | per source | `sources/markdown_docs.py`, `sources/github.py`, `sources/postgres_catalog.py` |
| Default processing per source family: one memoized child per ref, fetch on a miss, hand the record to the graph | per family, overridable per source | `sources/base.py` |
| The records a source produces | shared | `records.py` |
| Resolve references to entity keys; what the catalog says about each entity | shared | `catalog.py` |
| Write the node and its edges into the shared targets; the shared Entity and Person components | shared | `graph.py`, the `KnowledgeGraph` class |
| Registry, lifespan, app | glue | `main.py` |

**A `Document` is the contract between a document source and the pipeline.** It carries source, key, kind, title, text, url, author, status, and `refs` — entity keys the source already knows structurally. An entity source produces less: it lists the keys it knows, each with a change token, and what an entity *is* (name, kind, area, dependencies) comes from the catalog. Everything a source does is produce these.

**One adapter per source, in two halves.** Each adapter is a small frozen dataclass, one module per source type under `sources/`, with `refs()`, which lists the source's items cheaply, and for document sources `fetch(ref)`, which loads one item into a record. A ref is identity plus a change token — the `File` handle with its size and mtime, a catalog row's key and `xmin`, an issue number and its `updated_at` — and the processor is memoized on it, so an unchanged item is a memo hit and `fetch` never runs for it. `fetch` is a `@coco.fn`, so editing one adapter re-processes only that source's records. The GitHub adapters share one search helper and differ only in the qualifier and the field mapping; the pull-request adapter adds one line of source-specific extraction, reading the entity out of a conventional-commit title such as `fix(neo4j): …`. The registry is a list:

```python
sources: list[Source] = [
    PostgresCatalog("catalog", coco.use_context(CATALOG_DB), CATALOG_TABLE),
    MarkdownDocs("docs", DOCS_DIR),
    GitHubIssues("github_issues", GITHUB_REPO, GITHUB_MAX_ISSUES),
    GitHubPullRequests("github_pull_requests", GITHUB_REPO, GITHUB_MAX_PULL_REQUESTS),
    GitHubReleases("github_releases", GITHUB_REPO),
]
```

**Fetching is lazy.** The docs adapter's ref is the `File` itself, read only inside `fetch`. GitHub's REST listings already carry the item body, so the ref keeps the payload for `fetch` but fingerprints only the number and `updated_at`; a true metadata-only listing with batched body fetches is possible through GraphQL, which needs a token. The catalog adapter lists two columns per row, `key` and `xmin` (the row's last-writer transaction id, which changes on every update, so there is no `updated_at` column to maintain), and fetches nothing: the catalog is read once per run into two context values, and a row's component exists to hold the entity's node and to re-run when the row changes.

**The shared phase is one object.** `KnowledgeGraph` is mounted once and holds the six targets every source writes to: the `Entity`, `Document`, and `Person` node tables and the `DEPENDS_ON`, `MENTIONS`, and `AUTHORED` relation targets. Its `add_*` methods are the write steps, and both are `@coco.fn`, so editing them re-syncs every source through logic tracking. A document's own node and edges are declared outright; the nodes it only has a share of — the entities it mentions, the person who wrote it — it *holds* by mounting their shared component:

```python
class KnowledgeGraph:
    @coco.fn(memo=True)
    def entity_node(self, entity: Entity) -> None:            # the shared component {("entity", key)}
        self._entity_table.declare_record(row=EntityNode(key=entity.key, name=entity.name, kind=entity.kind, area=entity.area))
        entries = coco.use_context(ENTRIES)
        for dep in entity.depends_on:
            if dep in entries:
                self._depends_on_rel.declare_relation(from_id=entity.key, to_id=dep)

    @coco.fn(memo=True)
    def person_node(self, login: str) -> None:                 # the shared component {("person", login)}
        self._person_table.declare_record(row=PersonNode(login=login))

    async def _hold_entity(self, key: str) -> None:
        entity = coco.use_context(ENTRIES)[key]
        await coco.use_mount_shared(("entity", key), self.entity_node, entity)

    @coco.fn
    async def add_entity(self, key: str) -> None:
        await self._hold_entity(key)

    @coco.fn
    async def add_document(self, doc: Document) -> None:
        doc_id = f"{doc.source}:{doc.key}"
        self._document_table.declare_record(row=DocumentNode(id=doc_id, source=doc.source, kind=doc.kind, title=doc.title, ...))
        resolver = coco.use_context(RESOLVER)
        mentioned = resolver.resolve(f"{doc.title}\n{doc.text}")
        mentioned.update(ref for ref in doc.refs if ref in resolver.keys)
        for key in sorted(mentioned):
            await self._hold_entity(key)
            self._mentions_rel.declare_relation(from_id=doc_id, to_id=key)
        if doc.author:
            await coco.use_mount_shared(("person", doc.author), self.person_node, doc.author)
            self._authored_rel.declare_relation(from_id=doc.author, to_id=doc_id)
```

**Processing lives on the source.** The two family base classes in `sources/base.py` supply the default: `process` is one memoized child component per ref — for a document, fetch on a miss and hand the record to the graph; for an entity, hold its shared node — and `ingest` is the source's own processing component, which mounts those children. A source module writes `refs()`, plus `fetch()` for documents, and inherits the rest. A source whose records need more than fetch-then-add, say one that mounts a child per section of a long page, overrides `process` in its own module and still ends by calling `add_document`, so the targets and the write logic stay shared.

```python
@dataclass(frozen=True)
class DocumentSource(ABC, Generic[RefT]):
    name: str

    @abstractmethod
    def refs(self) -> AsyncIterable[tuple[coco.StableKey, RefT]]: ...

    @abstractmethod
    async def fetch(self, ref: RefT) -> Document: ...

    @coco.fn(memo=True)
    async def process(self, ref: RefT, graph: KnowledgeGraph) -> None:
        await graph.add_document(await self.fetch(ref))   # fetch only runs on a memo miss

    @coco.fn
    async def ingest(self, graph: KnowledgeGraph) -> None:
        await coco.mount_each(coco.component_subpath("record"), self.process, self.refs(), graph)
```

**Two layers of processing components, and a shared namespace beside them.** Each source's `ingest` is a [processing component](https://cocoindex.io/docs/programming_guide/processing_component/) at `/source/<name>` that lists its refs, and every ref is a memoized child below it. The shared components live outside that tree, keyed rather than pathed:

```
/source/"catalog"/"record"/"neo4j"
/source/"docs"/"record"/"connectors/neo4j.mdx"
/source/"github_issues"/"record"/2460
/source/"github_pull_requests"/"record"/2491
/source/"github_releases"/"record"/"v1.0.25"
{("entity", "neo4j")}            held by the catalog row and by every document above that mentions it
{("person", "georgeh0")}         held by every issue, pull request, and release that login authored
```

```python
@coco.fn
async def app_main() -> None:
    sources: list[Source] = [...]   # the registry above
    graph = await KnowledgeGraph.mount(KG_DB)
    with coco.component_subpath("source"):
        for source in sources:
            await coco.mount(coco.component_subpath(source.name), source.ingest, graph)
```

The source layer is what makes many sources manageable: sources enumerate in parallel, a source that fails (a rate limit, an unreachable API) fails in its own component while the others finish and keeps what it built last time, and removing a source from the registry unmounts its subtree, taking every node and edge it owned — and every hold it had on a shared node — with it.

**Shared nodes have holders, not an owner.** Every `use_mount_shared(("entity", key), …)` records a hold from the mounting component to that key; the shared component is built once (it is memoized) and every later mount is a hit that adds a hold. When a component stops mounting the key — a document re-runs and no longer mentions the entity, an issue drops out of the listing and its component is unmounted, a catalog row is deleted — its hold is dropped. When the last hold is gone, the shared component and its node and edges are garbage-collected, exactly like a regular component losing its only parent. A memoized component that is a hit keeps its holds without running. So an `Entity` exists while the catalog describes it *or* anything mentions it, a `Person` exists while any document they authored does, and no source has to know what the others hold. Every holder must mount a shared component the same way (its arguments are its memo key; if holders disagreed, the latest mount would win and they would rebuild it in turns), which is why the entity's properties come from one catalog snapshot that every holder reads.

**The catalog is two context values, and only one of them is change-detected.** `RESOLVER` is the alias table that `add_document` resolves against; it has `detect_change=True`, so adding or removing an alias re-processes every document — each of them depends on it. `ENTRIES`, what the catalog says about each entity, is deliberately *not* change-detected: a memoized component inherits the dependencies of what it mounts, so tracking the entries would make every document that mentions an entity depend on the whole catalog, and editing one row's `area` would re-process hundreds of documents. Instead the row's `xmin` is the change token: an edited row re-runs its own component, which re-mounts the shared entity with the new properties as arguments, and the entity's memo misses. One edited row re-processes one row component and one entity component, and no document. The two are read in the lifespan, from the same table, with one query.

**Resolution is one seam.** `Resolver.resolve` in `catalog.py` does whole-word, case-insensitive alias matching against the catalog. Replace it with an embedding search, an LLM, or a call to your own entity-resolution service and nothing else changes.

## What you get

- **Adding a source is adding an adapter.** A module under `sources/` with a `refs()` that lists and, for documents, a `fetch()` that loads, and one line in the registry. The pipeline, the targets, and the graph schema do not change.
- **The shared phase has a name.** `KnowledgeGraph` holds the targets, the write steps, and the shared node components. Every source type ends by calling it, including one with its own processor, and editing it re-syncs every source.
- **A run with nothing changed fetches nothing.** One stat per file, one two-column query, the GitHub listings. No file reads, no row fetches, no conversions: the change token in each ref decides.
- **Shared labels, shared edges, shared nodes.** `MATCH (d:Document)-[:MENTIONS]->(e:Entity {key: 'postgres'})` returns docs pages, issues, pull requests, and releases in one result; `MATCH (p:Person)-[:AUTHORED]->(d)` joins them to who wrote them. Filter on `d.kind` when you want one of them.
- **Nodes several sources know about exist exactly as long as something knows them.** No orphan sweep, no single owning source, no write-order rules between sources: a hold per mention, and garbage collection when the last one goes.
- **No fan-in bottleneck.** Nothing collects "all entities" or "all people" into one component. A million documents are a million small incremental units, each holding the handful of shared nodes it touches.
- **Sources are isolated.** Each enumerates in its own processing component; a failure or a removal is scoped to that source's subtree.
- **Incremental across all of them.** Edit a page, close an issue, cut a release, change a catalog row: exactly those records re-run, and vanished records take their nodes, edges, and holds with them.
- **Three connector kinds in one app.** A database table, a directory walk, and an HTTP API, with no connector needed for the API: any async generator of keyed refs is a source.

## Run it

**1. Start Postgres and Neo4j:**

```sh
docker compose -f ../../dev/postgres.yaml up -d
docker run -d -p 7474:7474 -p 7687:7687 -e NEO4J_AUTH=neo4j/cocoindex --name cocoindex-neo4j neo4j:5.26-community
```

**2. Configure and install** (`cocoindex-plus` comes from the Keygen index; `pyproject.toml` names the two environment variables that hold your installation token):

```sh
cp .env.example .env     # defaults match the two containers above
uv sync
```

**3. Seed the catalog** — one row per CocoIndex component, with aliases and dependencies:

```sh
psql "$POSTGRES_URL" -f ./prepare_source_data.sql
```

**4. Build the graph** — reads the catalog, walks the docs folder, fetches the repo's issues, pull requests, and releases, and syncs Neo4j:

```sh
uv run cocoindex update main
```

One run stays within GitHub's unauthenticated search limit of ten requests a minute; set `GITHUB_TOKEN` in `.env` to run more often. Run it again and nothing is written: every record is memoized on its change token.

**5. Explore the graph** — open [Neo4j Browser](http://localhost:7474) (`neo4j` / `cocoindex`). Every query below spans sources; that is the point of the shared labels:

```cypher
// One entity, seen from every kind of source
MATCH (d:Document)-[:MENTIONS]->(:Entity {key: 'postgres'})
RETURN d.kind AS kind, count(*) AS documents, collect(d.title)[..3] AS sample
ORDER BY documents DESC
```

```cypher
// Who works on an entity, and in what form: the people behind its issues, pull requests, and releases
MATCH (p:Person)-[:AUTHORED]->(d:Document)-[:MENTIONS]->(:Entity {key: 'neo4j'})
RETURN p.login AS person, d.kind AS kind, count(*) AS documents
ORDER BY person, kind
```

```cypher
// For each entity: the last release that mentioned it and how many issues are still open
MATCH (e:Entity)<-[:MENTIONS]-(r:Document {kind: 'release'})
WITH e, max(r.updated_at) AS last_release
OPTIONAL MATCH (e)<-[:MENTIONS]-(i:Document {kind: 'issue', status: 'open'})
RETURN e.key AS entity, last_release, count(i) AS open_issues
ORDER BY open_issues DESC
```

```cypher
// Open issues and pull requests about anything the Rust SDK depends on, transitively
MATCH (:Entity {key: 'rust_sdk'})-[:DEPENDS_ON*1..]->(e:Entity)<-[:MENTIONS]-(d:Document {status: 'open'})
RETURN e.key AS entity, d.kind AS kind, d.title AS title
ORDER BY entity, kind
```

```cypher
// Which area of the project do open issues cluster in?
MATCH (e:Entity)<-[:MENTIONS]-(i:Document {kind: 'issue', status: 'open'})
RETURN e.area AS area, count(DISTINCT i) AS open_issues, collect(DISTINCT e.key) AS entities
ORDER BY open_issues DESC
```

```cypher
// People who both report and fix: authored an open issue and a pull request about the same entity
MATCH (p:Person)-[:AUTHORED]->(:Document {kind: 'issue', status: 'open'})-[:MENTIONS]->(e:Entity)<-[:MENTIONS]-(:Document {kind: 'pull_request'})<-[:AUTHORED]-(p)
RETURN p.login AS person, collect(DISTINCT e.key) AS entities
ORDER BY size(entities) DESC
```

```cypher
// Documents that cut across the most entities, whatever their source
MATCH (d:Document)-[:MENTIONS]->(e:Entity)
WITH d, collect(e.key) AS entities
WHERE size(entities) >= 3
RETURN d.source AS source, d.title AS title, entities
ORDER BY size(entities) DESC LIMIT 10
```

## Make it yours

- **Add a source.** Write a module under `sources/` with a `DocumentSource` subclass whose `refs()` yields `(key, ref)` pairs where the ref carries a change token, and whose `@coco.fn` `fetch(ref)` returns a `Document`, then append it to `sources` in `app_main`. A Jira project or a Confluence space is the same shape, and both list items with version numbers and can fetch several per request, so their `fetch` can be a batched `@coco.fn.as_async(batching=True)` body. A source with its own extraction step (an LLM prompt per source type, a parser for its format) does that work inside `fetch` and still returns a `Document`. Its authors become `Person` nodes, held alongside everyone else's, as long as the source names them by the same login.
- **Add an entity source.** A service catalog, a service mesh export, or an HR directory is an `EntitySource` subclass that yields refs with a `key`. Two entity sources that know the same key hold the same node: there is no owner to collide with.
- **Give a source its own processing.** When fetch-then-add is not enough, override `process` in that source's module. It still receives the `KnowledgeGraph` and ends with `add_document` or `add_entity`, so the targets and the write logic stay shared.
- **Point it at your data.** `DOCS_DIR` for any Markdown or MDX tree; `GITHUB_REPO`, `GITHUB_TOKEN`, and for GitHub Enterprise `GITHUB_API_URL=https://<host>/api/v3`; `prepare_source_data.sql` for your catalog, keeping a key, an `aliases` array, and a `depends_on` array.
- **Swap the resolver.** `Resolver.resolve` is where whole-word matching becomes an embedding search, an LLM call, or a lookup against your own entity-resolution service.

**What follows from the catalog being the resolver.** Text mentions are found through the catalog's aliases, so deleting a catalog row also ends the text mentions of that entity on the next run: every document re-resolves, the row's hold and the documents' holds go together, and the node is garbage-collected. A structured reference (`refs`) only counts when the catalog knows the key, which keeps pull-request scopes such as `chore(deps)` from becoming entities. To let an entity outlive its catalog row, give it a holder that does not go through the resolver — a second entity source that lists it, or a structured reference admitted without the catalog check.

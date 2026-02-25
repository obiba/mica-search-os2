# mica-search-opensearch — Implementation Plan

## Context

This plugin implements the Mica search SPI for OpenSearch. It is a drop-in replacement for
`mica-search-es8` targeting an OpenSearch cluster instead of Elasticsearch.

**No changes are required to the Mica SPI or Mica server.** This plugin is compatible with
**Mica >= 6.0** out of the box — it implements the same `SearchEngineService` interface,
registered via the same Java ServiceLoader mechanism.

**Source to clone:** https://github.com/obiba/mica-search-es8

**Estimated effort: 2 days**
- Day 1: Project setup, dependency swap, global import renames, first successful compile
- Day 2: Range query fix, integration testing, packaging

---

## Implementation Steps

### Step 1 — Create the Maven Project

Create a new repository `mica-search-opensearch` and copy the entire `src/` tree from `mica-search-es8`.

```
groupId:    org.obiba.es
artifactId: mica-search-opensearch
version:    1.0-SNAPSHOT
java:       21
```

Rename config files:
- `src/main/conf/elasticsearch.yml` → `src/main/conf/opensearch.yml`
- `src/main/conf/plugin.properties` — update `name`, `title`, `description`, replace `es.version` with `opensearch.version`

---

### Step 2 — Replace Dependencies in pom.xml

| Remove | Add |
|--------|-----|
| `co.elastic.clients:elasticsearch-java:8.x` | `org.opensearch.client:opensearch-java:2.x` |
| `org.elasticsearch.client:elasticsearch-rest-high-level-client:8.x` | `org.opensearch.client:opensearch-rest-high-level-client:2.x` |
| `org.elasticsearch:elasticsearch:8.x` | `org.opensearch:opensearch:2.x` |

All other dependencies (`mica-spi:6.0.0`, `rql-parser`, Jackson, Guava, SLF4j) are unchanged.

Target OpenSearch version: **2.x latest stable** (2.19 as of early 2026).

---

### Step 3 — Rename Imports (Global Search-and-Replace)

Run these substitutions across all Java files:

**Package prefixes:**
```
co.elastic.clients.elasticsearch  →  org.opensearch.client.opensearch
co.elastic.clients.json           →  org.opensearch.client.json
co.elastic.clients.transport      →  org.opensearch.client.transport
org.elasticsearch.client          →  org.opensearch.client
org.elasticsearch.index           →  org.opensearch.index
org.elasticsearch.search          →  org.opensearch.search
```

**Individual classes:**
| Find | Replace |
|------|---------|
| `ElasticsearchClient` | `OpenSearchClient` |
| `org.elasticsearch.common.Strings` import | Delete — use `com.google.common.base.Strings` (already imported) |

After replacing, verify the project compiles: `mvn compile`. Fix any residual import errors.

---

### Step 4 — Fix Range Queries in RQLQuery.java (Manual)

This is the only file requiring a logic change, not just a rename.

The ES8 client splits range queries into typed variants (`TermRangeQuery` for strings/keywords).
OpenSearch uses a single unified `range` query for all field types — there is no `TermRangeQuery`.

**Replace every occurrence of this ES8 pattern:**
```java
Query.of(q -> q.range(r -> r.term(TermRangeQuery.of(t -> t.field(field).gte(value)))))
```

**With this OpenSearch pattern:**
```java
Query.of(q -> q.range(RangeQuery.of(r -> r.field(field).gte(JsonData.of(value)))))
```

Add imports:
```java
import org.opensearch.client.opensearch._types.query_dsl.RangeQuery;
import org.opensearch.client.json.JsonData;
```
Remove import: `TermRangeQuery`

Apply to these 6 methods in `RQLQuery.RQLQueryBuilder`:

| Method | Bounds to set |
|--------|--------------|
| `visitLe` | `.lte(JsonData.of(value))` |
| `visitLt` | `.lt(JsonData.of(value))` |
| `visitGe` | `.gte(JsonData.of(value))` |
| `visitGt` | `.gt(JsonData.of(value))` |
| `visitBetween` | `.gte(JsonData.of(values[0])).lte(JsonData.of(values[1]))` |
| `visitInRangeInternal` | `.gte(JsonData.of(values[0]))` and/or `.lt(JsonData.of(values[1]))` per branch |

---

### Step 5 — Fix Client Instantiation in ESSearchEngineService.java (Manual)

One line change:
```java
// Remove:
ElasticsearchClient client = new ElasticsearchClient(transport);

// Add:
OpenSearchClient client = new OpenSearchClient(transport);
```

`RestClientTransport` and `JacksonJsonpMapper` are used identically in both clients.

---

### Step 6 — Update Packaging

In `src/main/assembly/plugin.xml`, replace Elasticsearch artifact references with the
OpenSearch equivalents so the ZIP bundles the correct runtime JARs.

`src/main/resources/META-INF/services/org.obiba.mica.spi.search.SearchEngineService` —
**no change needed**, the service class name is unchanged.

---

## QA

### Build
- [ ] `mvn compile` — zero errors
- [ ] `mvn package` — produces a ZIP artifact

### Integration (requires a running OpenSearch 2.x instance)
- [ ] Plugin loads and connects successfully
- [ ] Index creation completes for all 8 entity types (Variable, Dataset, Study, Network, File, Person, Project, Taxonomy)
- [ ] Bulk indexing completes without errors
- [ ] Basic search returns results
- [ ] Faceted search returns correct aggregation buckets
- [ ] Range queries return correct results (test with numeric and string fields)
- [ ] `harmonizationStatusAggregation` returns correct nested structure
- [ ] Plugin unloads cleanly (stop/restart Mica)

### Regression (against a Mica 6.x instance)
- [ ] Existing Mica search UI functions identically to the ES8 plugin
- [ ] No data loss after reindex

---

## Notes for Claude Code

1. Clone `obiba/mica-search-es8` as the starting point
2. Steps 3 is mechanical — use global search-and-replace
3. Step 4 is the only non-trivial change — edit `RQLQuery.java` only
4. Use `mcp__context7__resolve-library-id` to look up `opensearch-java` API docs if needed

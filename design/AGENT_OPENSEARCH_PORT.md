# Agent Instructions: OpenSearch Port

## Starting Point

The project has been created and the source files copied from `mica-search-es8`.
The codebase is indexed. Use Serena tools to navigate — do not read full files unless necessary.

This task is mechanical. Work file by file, compile often, fix errors as they appear.
**Target: zero compiler errors, then run the QA checklist.**

---

## What This Plugin Is

A Mica search plugin. It implements `org.obiba.mica.spi.search.SearchEngineService` (from `mica-spi`)
and communicates with a search cluster over HTTP using a Java client library.

The only work here is **replacing the Elasticsearch Java client with the OpenSearch Java client**.
No logic changes. No SPI changes. The Mica server does not need to be modified.

---

## Step 1 — pom.xml

Replace these three dependencies:

| Remove | Add |
|--------|-----|
| `co.elastic.clients:elasticsearch-java:8.x` | `org.opensearch.client:opensearch-java:2.x` |
| `org.elasticsearch.client:elasticsearch-rest-high-level-client:8.x` | `org.opensearch.client:opensearch-rest-high-level-client:2.x` |
| `org.elasticsearch:elasticsearch:8.x` | `org.opensearch:opensearch:2.x` |

Use the latest stable `2.x` version for all three. All other dependencies are unchanged.

Run `mvn compile` after this step. It will fail with import errors — that is expected.

---

## Step 2 — Global Import Renames

Use Serena `search_for_pattern` or grep across all `.java` files.

**Package prefix substitutions:**
```
co.elastic.clients.elasticsearch  →  org.opensearch.client.opensearch
co.elastic.clients.json           →  org.opensearch.client.json
co.elastic.clients.transport      →  org.opensearch.client.transport
org.elasticsearch.client          →  org.opensearch.client
org.elasticsearch.index           →  org.opensearch.index
org.elasticsearch.search          →  org.opensearch.search
```

**Individual replacements:**
| Find | Replace |
|------|---------|
| `ElasticsearchClient` | `OpenSearchClient` |
| `import org.elasticsearch.common.Strings;` | delete — `com.google.common.base.Strings` is already imported |

Run `mvn compile`. There will be one remaining error in `RQLQuery.java` — see Step 3.

---

## Step 3 — Fix RQLQuery.java (only file needing logic change)

Use Serena to read the body of the `RQLQueryBuilder` inner class in `RQLQuery`.

`TermRangeQuery` does not exist in the OpenSearch client. Replace every usage with `RangeQuery`.

**Pattern to replace** (6 occurrences across `visitLe`, `visitLt`, `visitGe`, `visitGt`,
`visitBetween`, `visitInRangeInternal`):

```java
// Remove:
import ...TermRangeQuery;
Query.of(q -> q.range(r -> r.term(TermRangeQuery.of(t -> t.field(field).gte(value)))))

// Add:
import org.opensearch.client.opensearch._types.query_dsl.RangeQuery;
import org.opensearch.client.json.JsonData;
Query.of(q -> q.range(RangeQuery.of(r -> r.field(field).gte(JsonData.of(value)))))
```

Wrap all bound values in `JsonData.of()`. The `RangeQuery.Builder` methods are:
`.field(String)`, `.gte(JsonData)`, `.gt(JsonData)`, `.lte(JsonData)`, `.lt(JsonData)`

Run `mvn compile` — should be clean.

---

## Step 4 — ESSearchEngineService.java (one line)

Use Serena to find the client instantiation. Change:
```java
ElasticsearchClient client = new ElasticsearchClient(transport);
```
to:
```java
OpenSearchClient client = new OpenSearchClient(transport);
```

---

## Step 5 — Config and Metadata

- Rename `src/main/conf/elasticsearch.yml` → `src/main/conf/opensearch.yml`
- In `plugin.properties`: update `name`, `title`, `description`; rename `es.version` to `opensearch.version`
- In `src/main/assembly/plugin.xml`: replace Elasticsearch JAR references with OpenSearch equivalents
- `META-INF/services/org.obiba.mica.spi.search.SearchEngineService` — **no change**

Run `mvn package` — should produce a ZIP.

---

## QA Checklist

### Build
- [ ] `mvn compile` — zero errors
- [ ] `mvn package` — ZIP artifact produced

### Integration (requires running OpenSearch 2.x + Mica 6.x)
- [ ] Plugin connects to OpenSearch on startup
- [ ] All 8 indices created (Variable, Dataset, Study, Network, File, Person, Project, Taxonomy)
- [ ] Bulk indexing completes without errors
- [ ] Basic keyword search returns results
- [ ] Faceted search returns correct aggregation buckets
- [ ] Range filter returns correct results
- [ ] `harmonizationStatusAggregation` returns correct nested structure
- [ ] Plugin stops and restarts cleanly

### Regression
- [ ] Mica search UI behaves identically to the ES8 plugin

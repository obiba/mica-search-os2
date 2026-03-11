# mica-search-os2

[Mica](https://github.com/obiba/mica2) is OBiBa's web data portal application server.

`mica-search-os2` is Mica's search plugin for OpenSearch 2.x. It is a drop-in replacement for `mica-search-es8`, compatible with Mica >= 6.0.

## Installation

### 1. Configure Mica to use this plugin

In Mica's `application-prod.yml`, set:

```yaml
plugins:
  micaSearchPlugin: mica-search-os2
```

This tells Mica which search plugin to load. Without this setting, Mica will not use the plugin.

### 2. Configure the plugin

Edit `$MICA_HOME/plugins/mica-search-os2-<version>/plugin.properties` and set the OpenSearch cluster address:

```properties
# Comma-separated list of OpenSearch nodes (host:port). Default is localhost:9200.
transportAddresses=localhost:9200
```

### 3. Configure index settings (optional)

The plugin reads `$MICA_HOME/plugins/mica-search-os2-<version>/opensearch.yml` for index-level settings such as custom analyzers. Only `index:` settings are relevant here — node and discovery settings are not applicable since the plugin connects to an external cluster over HTTP.

Example `opensearch.yml`:

```yaml
index:
  max_ngram_diff: 18
  max_result_window: 1000000
  analysis:
    analyzer:
      mica_index_analyzer:
        type: custom
        char_filter: [html_strip]
        tokenizer: mica_char_group_tokenizer
        filter: [lowercase, mica_asciifolding_filter, mica_nGram_filter]
      mica_search_analyzer:
        type: custom
        char_filter: [html_strip]
        tokenizer: mica_char_group_tokenizer
        filter: [lowercase, mica_asciifolding_filter]
    filter:
      mica_asciifolding_filter:
        type: asciifolding
        preserve_original: true
      mica_nGram_filter:
        type: ngram
        min_gram: 2
        max_gram: 20
    tokenizer:
      mica_char_group_tokenizer:
        type: char_group
        tokenize_on_chars: ["whitespace", "-", "_"]
```

---

## Testing with Docker

The fastest way to get a local OpenSearch node for testing is via Docker.

### Run OpenSearch

```bash
docker run -d \
  --name opensearch \
  -p 9200:9200 \
  -e "discovery.type=single-node" \
  -e "DISABLE_SECURITY_PLUGIN=true" \
  opensearchproject/opensearch:2.19.0
```

- Port `9200` — REST API used by the plugin
- `discovery.type=single-node` — required for a single-node setup
- `DISABLE_SECURITY_PLUGIN=true` — disables TLS and authentication, suitable for local testing only

Verify OpenSearch is up:

```bash
curl http://localhost:9200
```

You should see a JSON response with `"cluster_name"` and `"version"` fields.

### Stop and remove the container

```bash
docker stop opensearch && docker rm opensearch
```

### Persist data between restarts (optional)

```bash
docker run -d \
  --name opensearch \
  -p 9200:9200 \
  -e "discovery.type=single-node" \
  -e "DISABLE_SECURITY_PLUGIN=true" \
  -v opensearch-data:/usr/share/opensearch/data \
  opensearchproject/opensearch:2.19.0
```

---

## Integration Tests

Integration tests verify the plugin against a real OpenSearch 2.x instance. They are not part of the standard build (`mvn install`) and must be run explicitly with the `it` profile.

### Requirements

- Docker must be installed and running

### Run

```bash
mvn verify -Pit
```

This will automatically:

1. Start an OpenSearch 2.x container on port `19200` (to avoid conflicts with any local instance)
2. Run the integration tests (`ESIndexerIT`, `ESSearcherIT`)
3. Stop and remove the container

### What is tested

- **`ESIndexerIT`** — indexing a single document, bulk indexing, dropping an index, deleting a document
- **`ESSearcherIT`** — finding all documents, filtering by term, filtering by range, counting, pagination and sorting

### Customizing the port

The default port `19200` can be overridden:

```bash
mvn verify -Pit -Dit.opensearch.port=29200
```

---

## Mailing list

Have a question? Ask on our mailing list!

obiba-users@googlegroups.com

[http://groups.google.com/group/obiba-users](http://groups.google.com/group/obiba-users)

## License

OBiBa software are open source and made available under the [GPL3 licence](http://www.obiba.org/node/62). OBiBa software are free of charge.

## OBiBa acknowledgments

If you are using OBiBa software, please cite our work in your code, websites, publications or reports.

"The work presented herein was made possible using the OBiBa suite (www.obiba.org), a software suite developed by Maelstrom Research (www.maelstrom-research.org)"

The default Study model included in Mica was designed by [Maelstrom Research](https://www.maelstrom-research.org) under the [Creative Commons License with Non Commercial and No Derivative](https://creativecommons.org/licenses/by-nc-nd/4.0/) constraints.

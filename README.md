# mica-search-opensearch

[Mica](https://github.com/obiba/mica2) is OBiBa's web data portal application server.

mica-search-opensearch is Mica's search plugin for OpenSearch 2.x servers. It is a drop-in replacement for `mica-search-es8`, compatible with Mica >= 6.0.

## Testing with Docker

The fastest way to get a local OpenSearch node for testing is via Docker.

### Run OpenSearch with Docker

```bash
docker run -d \
  --name opensearch \
  -p 9200:9200 \
  -p 9600:9600 \
  -e "discovery.type=single-node" \
  -e "DISABLE_SECURITY_PLUGIN=true" \
  opensearchproject/opensearch:2.19.0
```

- Port `9200` — REST API (used by this plugin)
- Port `9600` — Performance Analyzer (optional)
- `discovery.type=single-node` — required for a single-node cluster
- `DISABLE_SECURITY_PLUGIN=true` — disables TLS and auth, suitable for local testing only

Wait a few seconds for OpenSearch to start, then verify it is up:

```bash
curl http://localhost:9200
```

You should see a JSON response with `"cluster_name"` and `"version"` fields.

### Stop and remove the container

```bash
docker stop opensearch && docker rm opensearch
```

### Persisting data between restarts (optional)

If you want index data to survive container restarts, mount a volume:

```bash
docker run -d \
  --name opensearch \
  -p 9200:9200 \
  -e "discovery.type=single-node" \
  -e "DISABLE_SECURITY_PLUGIN=true" \
  -v opensearch-data:/usr/share/opensearch/data \
  opensearchproject/opensearch:2.19.0
```

### Configure Mica to use this instance

Point the plugin at your local OpenSearch node by setting the following in Mica's configuration:

```
opensearch.host=localhost
opensearch.port=9200
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

/*
 * Copyright (c) 2024 OBiBa. All rights reserved.
 *
 * This program and the accompanying materials
 * are made available under the terms of the GNU Public License v3.0.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.obiba.es.mica;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import net.minidev.json.JSONObject;
import org.apache.http.ConnectionClosedException;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.xcontent.XContentType;
import org.obiba.es.mica.mapping.DatasetIndexConfiguration;
import org.obiba.es.mica.mapping.FileIndexConfiguration;
import org.obiba.es.mica.mapping.NetworkIndexConfiguration;
import org.obiba.es.mica.mapping.PersonIndexConfiguration;
import org.obiba.es.mica.mapping.ProjectIndexConfiguration;
import org.obiba.es.mica.mapping.StudyIndexConfiguration;
import org.obiba.es.mica.mapping.TaxonomyIndexConfiguration;
import org.obiba.es.mica.mapping.VariableIndexConfiguration;
import org.obiba.mica.spi.search.ConfigurationProvider;
import org.obiba.mica.spi.search.Indexer;
import org.obiba.mica.spi.search.SearchEngineService;
import org.obiba.mica.spi.search.Searcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public class ESSearchEngineService implements SearchEngineService {
  private final Logger log = LoggerFactory.getLogger(this.getClass());

  private static final String ES_BRANCH = "8.13.x";

  private static final String ES_CONFIG_FILE = "elasticsearch.yml";

  private static final int DEFAULT_MAX_RETIRES = 10;
  private static final int DEFAULT_INITIAL_BACKOFF = 1000; // Miliseconds
  private static final int DEFAULT_BACKOFF_MULTIPLIER = 2;

  private Properties properties;

  private boolean running;

  private Node esNode;

  private ElasticsearchClient client;

  private ESIndexer esIndexer;

  private ESSearcher esSearcher;

  private ConfigurationProvider configurationProvider;

  private Set<Indexer.IndexConfigurationListener> indexConfigurationListeners;

  private String indexSettings = "{}";

  private ObjectMapper yamlObjectMapper = new ObjectMapper(new YAMLFactory());

  private final AtomicBoolean stopRetries = new AtomicBoolean(false); // Flag to stop retries

  @Override
  public String getName() {
    return "mica-search-es8";
  }

  @Override
  public Properties getProperties() {
    return properties;
  }

  @Override
  public void configure(Properties properties) {
    this.properties = properties;
  }

  @Override
  public boolean isRunning() {
    return running;
  }


  public ESSearchEngineService() {
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      // Shutdown detected. Stopping Elasticsearch connection retries
      stopRetries.set(true);
    }));
  }

  @Override
  public void start() {
    // do init stuff
    if (properties != null) {
      Settings.Builder builder = getSettings();
      createTransportClient(builder);

      String bufferLimitBytes = builder.build().get("http.max_content_length_bytes");

      esIndexer = new ESIndexer(this);
      esSearcher = new ESSearcher(this, bufferLimitBytes == null || bufferLimitBytes.isEmpty() ? 250 * 1024 * 1024
        : Integer.parseInt(bufferLimitBytes));

      running = true;
    }
  }

  @Override
  public void stop() {
    running = false;
    if (esNode != null) {
      try {
        esNode.close();
      } catch (IOException e) {
        log.error("Failed to close node {}", e.getMessage());
      }
    }
    if (client != null) {
      client.shutdown();
    }
    esNode = null;
    client = null;
  }

  @Override
  public void setConfigurationProvider(ConfigurationProvider configurationProvider) {
    this.configurationProvider = configurationProvider;
  }

  @Override
  public Indexer getIndexer() {
    return esIndexer;
  }

  @Override
  public Searcher getSearcher() {
    return esSearcher;
  }

  public ElasticsearchClient getClient() {
    return client;
  }

  ConfigurationProvider getConfigurationProvider() {
    return configurationProvider;
  }

  ObjectMapper getObjectMapper() {
    if (configurationProvider == null || configurationProvider.getObjectMapper() == null) {
      ObjectMapper mapper = new ObjectMapper();
      mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
      mapper.findAndRegisterModules();
      return mapper;
    }
    return configurationProvider.getObjectMapper();
  }

  synchronized Set<Indexer.IndexConfigurationListener> getIndexConfigurationListeners() {
    if (indexConfigurationListeners == null) {
      indexConfigurationListeners = Sets.newHashSet();
      indexConfigurationListeners.add(new VariableIndexConfiguration(configurationProvider));
      indexConfigurationListeners.add(new DatasetIndexConfiguration(configurationProvider));
      indexConfigurationListeners.add(new StudyIndexConfiguration(configurationProvider));
      indexConfigurationListeners.add(new NetworkIndexConfiguration(configurationProvider));
      indexConfigurationListeners.add(new FileIndexConfiguration(configurationProvider));
      indexConfigurationListeners.add(new PersonIndexConfiguration(configurationProvider));
      indexConfigurationListeners.add(new ProjectIndexConfiguration(configurationProvider));
      indexConfigurationListeners.add(new TaxonomyIndexConfiguration(configurationProvider));
    }
    return indexConfigurationListeners;
  }

  int getNbShards() {
    return getIntProperty("shards", 5);
  }

  int getNbReplicas() {
    return getIntProperty("replicas", 1);
  }

  //
  // Private methods
  //

  public void createTransportClient(Settings.Builder builder) {
    builder.put("client.transport.sniff", isTransportSniff());

    int maxRetries = getIntProperty("maxRetries", DEFAULT_MAX_RETIRES);
    long initialBackoffMs = getIntProperty("initialBackoff", DEFAULT_INITIAL_BACKOFF);
    int backoffMultiplier = getIntProperty("backoffMultiplier", DEFAULT_BACKOFF_MULTIPLIER);

    // Make sure app is not blocked
    Thread.ofVirtual().start(() -> retryConnection(maxRetries, initialBackoffMs, backoffMultiplier));
  }

  private HttpHost[] getHttpHosts() {
    List<String> transportAddresses = getTransportAddresses();
    HttpHost[] httpHosts = transportAddresses.stream()
      .map(transportAddress -> {
        int port = 9200;
        String host = transportAddress;
        int sepIdx = transportAddress.lastIndexOf(':');

        if (sepIdx > 0) {
          port = Integer.parseInt(transportAddress.substring(sepIdx + 1));
          host = transportAddress.substring(0, sepIdx);
        }

        return new HttpHost(host, port, "http");
      })
      .toArray(HttpHost[]::new);
    return httpHosts;
  }

  private void retryConnection(int maxRetries, long initialBackoffMs, int backoffMultiplier) {
    try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
      executor.submit(() -> {
        HttpHost[] httpHosts = getHttpHosts();
        int attempt = 0;
        long backoff = initialBackoffMs;

        while (attempt < maxRetries && !stopRetries.get()) {
          try {
            RestClient restClient = RestClient.builder(httpHosts).build();
            RestClientTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
            client = new ElasticsearchClient(transport);

            if (client.ping().value()) {
              log.info("Connected to Elasticsearch successfully!");
              return;
            }

            throw new IOException("Ping failed - Elasticsearch might not be ready");

          } catch (IOException e) {
            attempt++;
            log.warn("Attempt {}/{} failed: {}", attempt, maxRetries, e.getMessage());

            if (!isRetryableException(e)) {
              log.error("Non-retryable error detected, stopping connection attempts.", e);
              break;
            }

            if (attempt < maxRetries && !stopRetries.get()) {
              try {
                long nextDelay = backoff;
                log.info("Retrying in {}ms...", nextDelay);
                Thread.sleep(nextDelay);
                backoff *= backoffMultiplier; // Exponential backoff
              } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return;
              }
            }
          }
        }

        if (stopRetries.get()) {
          log.info("Retrying stopped due to server shutdown.");
        } else {
          log.error("Failed to connect to Elasticsearch after {} attempts.", maxRetries);
        }
      });
    }
  }

  private boolean isRetryableException(IOException e) {
    return e instanceof ConnectException || e instanceof UnknownHostException || e instanceof ConnectionClosedException;
  }

  private boolean isDataNode() {
    return Boolean.parseBoolean(properties.getProperty("dataNode", "true"));
  }

  private String getClusterName() {
    return properties.getProperty("clusterName", "mica");
  }

  private List<String> getTransportAddresses() {
    String addStr = properties.getProperty("transportAddresses", "").trim();
    return addStr.isEmpty() ? Lists.newArrayList("localhost:9200")
      : Stream.of(addStr.split(",")).map(String::trim).collect(toList());
  }

  private boolean isTransportClient() {
    return Boolean.parseBoolean(properties.getProperty("transportClient", "false"));
  }

  private boolean isTransportSniff() {
    return Boolean.parseBoolean(properties.getProperty("transportSniff", "false"));
  }

  private File getWorkFolder() {
    return getServiceFolder(WORK_DIR_PROPERTY);
  }

  private File getInstallFolder() {
    return getServiceFolder(INSTALL_DIR_PROPERTY);
  }

  private File getServiceFolder(String dirProperty) {
    String defaultDir = new File(".").getAbsolutePath();
    String dataDirPath = properties.getProperty(dirProperty, defaultDir);
    File dataDir = new File(dataDirPath);
    if (!dataDir.exists())
      dataDir.mkdirs();
    return dataDir;
  }

  String getIndexSettings() {
    return indexSettings;
  }

  private Settings.Builder getSettings() {
    File pluginWorkDir = new File(getWorkFolder(), properties.getProperty("es.version", ES_BRANCH));
    Settings.Builder builder = Settings.builder() //
      .put("path.home", getInstallFolder().getAbsolutePath()) //
      .put("path.data", new File(pluginWorkDir, "data").getAbsolutePath()) //
      .put("path.work", new File(pluginWorkDir, "work").getAbsolutePath());

    File defaultSettings = new File(getInstallFolder(), ES_CONFIG_FILE);
    if (defaultSettings.exists()) {
      try {
        builder.loadFromPath(defaultSettings.toPath());

        Map<String, Object> defaultSettingsMap = yamlObjectMapper.readValue(defaultSettings,
          new TypeReference<Map<String, Object>>() {
          });

        if (defaultSettingsMap.containsKey("index")) {
          Object defaultSettingsIndexObject = defaultSettingsMap.get("index");

          if (defaultSettingsIndexObject instanceof Map) {
            indexSettings = new JSONObject((Map) defaultSettingsIndexObject).toJSONString();
          }
        }

      } catch (IOException e) {
        log.error("Failed to load default settings {}", e);
      }
    }

    String settings = properties.getProperty("settings", "");
    if (!Strings.isNullOrEmpty(settings) && settings.indexOf(':') != -1) {
      builder.loadFromSource(settings, XContentType.YAML);
    }

    builder.put("cluster.name", getClusterName());

    return builder;
  }

  private int getIntProperty(String key, int defaultValue) {
    try {
      return Integer.parseInt(properties.getProperty(key, String.valueOf(defaultValue)));
    } catch (NumberFormatException e) {
      log.warn("Invalid value for '{}', using default: {}", key, defaultValue);
      return defaultValue;
    }
  }
}

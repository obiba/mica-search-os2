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

import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.transport.rest_client.RestClientTransport;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import net.minidev.json.JSONObject;
import org.apache.http.ConnectionClosedException;
import org.apache.http.HttpHost;
import org.opensearch.client.RestClient;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public class ESSearchEngineService implements SearchEngineService {
  private final Logger log = LoggerFactory.getLogger(this.getClass());

  private static final String OPENSEARCH_CONFIG_FILE = "opensearch.yml";

  private static final int DEFAULT_MAX_RETIRES = 10;
  private static final int DEFAULT_INITIAL_BACKOFF = 1000; // Milliseconds
  private static final int DEFAULT_BACKOFF_MULTIPLIER = 2;

  private Properties properties;

  private boolean running;

  private OpenSearchClient client;

  private ESIndexer esIndexer;

  private ESSearcher esSearcher;

  private ConfigurationProvider configurationProvider;

  private Set<Indexer.IndexConfigurationListener> indexConfigurationListeners;

  private String indexSettings = "{}";

  private ObjectMapper yamlObjectMapper = new ObjectMapper(new YAMLFactory());

  private final AtomicBoolean stopRetries = new AtomicBoolean(false);

  @Override
  public String getName() {
    return "mica-search-os2";
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
      stopRetries.set(true);
    }));
  }

  @Override
  public void start() {
    log.info("Starting mica-search-os2 plugin...");
    if (properties == null) {
      log.error("Plugin properties are null - plugin was not configured. Check that plugin.properties is present in the plugin directory.");
      return;
    }
    // JsonpUtils has a static initializer that calls JsonProvider.provider() via ServiceLoader.
    // In a plugin classloader environment, ServiceLoader uses the thread context classloader,
    // which is Mica's classloader and cannot see parsson in our lib/.
    // Fix: set the plugin classloader as the context classloader before triggering JsonpUtils
    // static init, so ServiceLoader can find parsson. This must happen before any OpenSearch
    // client code runs.
    ClassLoader pluginCl = getClass().getClassLoader();
    ClassLoader prevCl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(pluginCl);
      pluginCl.loadClass("org.opensearch.client.json.JsonpUtils");
      log.info("JsonpUtils initialized successfully with plugin classloader.");
    } catch (Exception e) {
      log.warn("Could not pre-initialize JsonpUtils: {}", e.getMessage());
    } finally {
      Thread.currentThread().setContextClassLoader(prevCl);
    }


    loadIndexSettings();
    createTransportClient();

    esIndexer = new ESIndexer(this);
    esSearcher = new ESSearcher(this, 250 * 1024 * 1024);

    running = true;
    log.info("mica-search-os2 plugin started, waiting for OpenSearch connection...");
  }



  @Override
  public void stop() {
    running = false;
    if (client != null) {
      client.shutdown();
    }
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

  public OpenSearchClient getClient() {
    return client;
  }

  /**
   * Executes the given callable with the plugin classloader set as the thread context classloader.
   * This is required because OpenSearch client's JsonpUtils uses ServiceLoader with the thread
   * context classloader, which on external threads (Jetty, Spring) is Mica's classloader and
   * cannot find parsson from the plugin's lib/.
   */
  <T> T withPluginClassLoader(java.util.concurrent.Callable<T> callable) {
    ClassLoader prev = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
    try {
      return callable.call();
    } catch (RuntimeException | Error e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      Thread.currentThread().setContextClassLoader(prev);
    }
  }

  void withPluginClassLoader(Runnable runnable) {
    ClassLoader prev = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
    try {
      runnable.run();
    } finally {
      Thread.currentThread().setContextClassLoader(prev);
    }
  }

  public org.opensearch.client.RestClient getRestClient() {
    return ((org.opensearch.client.transport.rest_client.RestClientTransport) client._transport()).restClient();
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

  public void createTransportClient() {
    int maxRetries = getIntProperty("maxRetries", DEFAULT_MAX_RETIRES);
    long initialBackoffMs = getIntProperty("initialBackoff", DEFAULT_INITIAL_BACKOFF);
    int backoffMultiplier = getIntProperty("backoffMultiplier", DEFAULT_BACKOFF_MULTIPLIER);

    Thread.ofVirtual().start(() -> retryConnection(maxRetries, initialBackoffMs, backoffMultiplier));
  }

  private HttpHost[] getHttpHosts() {
    List<String> transportAddresses = getTransportAddresses();
    return transportAddresses.stream()
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
  }

  private void retryConnection(int maxRetries, long initialBackoffMs, int backoffMultiplier) {
    // Ensure the plugin classloader is the context classloader so that ServiceLoader
    // (used by JsonpUtils static init to find jakarta.json.spi.JsonProvider) can find parsson.
    Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
    HttpHost[] httpHosts = getHttpHosts();
    log.info("Connecting to OpenSearch at {}...", Arrays.toString(httpHosts));

    int attempt = 0;
    long backoff = initialBackoffMs;

    while (attempt < maxRetries && !stopRetries.get()) {
      try {
        RestClient restClient = RestClient.builder(httpHosts).build();
        RestClientTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        client = new OpenSearchClient(transport);

        if (client.ping().value()) {
          log.info("Connected to OpenSearch successfully!");
          return;
        }

        throw new IOException("Ping failed - OpenSearch might not be ready");

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
            backoff *= backoffMultiplier;
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
      log.error("Failed to connect to OpenSearch after {} attempts.", maxRetries);
    }
  }


  private boolean isRetryableException(IOException e) {
    return e instanceof ConnectException || e instanceof UnknownHostException || e instanceof ConnectionClosedException;
  }

  private List<String> getTransportAddresses() {
    String addStr = properties.getProperty("transportAddresses", "").trim();
    return addStr.isEmpty() ? Lists.newArrayList("localhost:9200")
      : Stream.of(addStr.split(",")).map(String::trim).collect(toList());
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

  private void loadIndexSettings() {
    File defaultSettings = new File(getInstallFolder(), OPENSEARCH_CONFIG_FILE);
    if (defaultSettings.exists()) {
      try {
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

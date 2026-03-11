/*
 * Copyright (c) 2026 OBiBa. All rights reserved.
 *
 * This program and the accompanying materials
 * are made available under the terms of the GNU Public License v3.0.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.obiba.es.mica;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.obiba.es.mica.ESSearchEngineService;
import org.opensearch.client.opensearch.OpenSearchClient;

import java.util.concurrent.Callable;

/**
 * Minimal test double for ESSearchEngineService that wraps a pre-built OpenSearchClient
 * (connected to a Testcontainers OpenSearch instance).
 */
class ESSearchEngineServiceTestDouble extends ESSearchEngineService {

  private final OpenSearchClient testClient;

  ESSearchEngineServiceTestDouble(OpenSearchClient testClient) {
    this.testClient = testClient;
  }

  @Override
  public OpenSearchClient getClient() {
    return testClient;
  }

  @Override
  public ObjectMapper getObjectMapper() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    mapper.findAndRegisterModules();
    return mapper;
  }

  @Override
  int getNbShards() {
    return 1;
  }

  @Override
  int getNbReplicas() {
    return 0;
  }

  @Override
  <T> T withPluginClassLoader(Callable<T> callable) {
    try {
      return callable.call();
    } catch (RuntimeException | Error e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}

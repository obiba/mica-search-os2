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

import org.apache.http.HttpHost;
import org.junit.BeforeClass;
import org.opensearch.client.RestClient;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.transport.rest_client.RestClientTransport;

public abstract class AbstractOpenSearchIT {

  static OpenSearchClient client;
  static ESSearchEngineService service;
  static ESIndexer indexer;
  static ESSearcher searcher;

  @BeforeClass
  public static void setup() throws Exception {
    int port = Integer.parseInt(System.getProperty("it.opensearch.port", "19200"));
    RestClient restClient = RestClient.builder(new HttpHost("localhost", port, "http")).build();
    RestClientTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
    client = new OpenSearchClient(transport);

    service = new ESSearchEngineServiceTestDouble(client);
    indexer = new ESIndexer(service);
    searcher = new ESSearcher(service);
  }
}

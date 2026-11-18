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

import org.junit.BeforeClass;
import org.junit.Test;
import org.obiba.mica.spi.search.Searcher;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.opensearch.client.Request;
import org.opensearch.client.RestClient;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class ESSearcherSortIT extends AbstractOpenSearchIT {

  private static final String INDEX = "it_sort_studies";

  // the normalizer of opensearch.yml and the mapping produced by AbstractIndexConfiguration for a sortable field
  private static final String INDEX_BODY = """
      {
        "settings": {"analysis": {"normalizer": {"lowercase_normalizer": {
          "type": "custom", "filter": ["lowercase", "asciifolding"]}}}},
        "mappings": {"properties": {"acronym": {"properties": {"en": {
          "type": "keyword", "fields": {"sort": {"type": "keyword", "normalizer": "lowercase_normalizer"}}}}}}}
      }
      """;

  @BeforeClass
  public static void indexDocuments() throws Exception {
    setup();
    try {
      client.indices().delete(d -> d.index(INDEX));
    } catch (Exception ignore) {
      // not there
    }
    int port = Integer.parseInt(System.getProperty("it.opensearch.port", "19200"));
    try (RestClient restClient = RestClient.builder(new HttpHost("localhost", port, "http")).build()) {
      Request request = new Request("PUT", "/" + INDEX);
      request.setEntity(new StringEntity(INDEX_BODY, ContentType.APPLICATION_JSON));
      restClient.performRequest(request);
    }
    String[] acronyms = {"zeta", "Beta", "Éva", "alpha", "gamma", "Delta", "Epsilon"};
    for (int i = 0; i < acronyms.length; i++) {
      String acronym = acronyms[i];
      String id = "s" + i;
      client.index(r -> r.index(INDEX).id(id).document(Map.of("acronym", Map.of("en", acronym))));
    }
    client.indices().refresh(r -> r.index(INDEX));
  }

  private static List<String> acronyms(String rql) {
    return searcher.find(INDEX, "study", rql, null).getDocuments().stream()
        .map(d -> (String) ((Map<?, ?>) d.getSource().get("acronym")).get("en")).toList();
  }

  @Test
  public void test_sort_is_case_and_accent_insensitive() {
    assertThat(acronyms("study(limit(0,10),sort(acronym.en))"))
        .containsExactly("alpha", "Beta", "Delta", "Epsilon", "Éva", "gamma", "zeta");
  }

  @Test
  public void test_sort_desc() {
    assertThat(acronyms("study(limit(0,10),sort(-acronym.en))"))
        .containsExactly("zeta", "gamma", "Éva", "Epsilon", "Delta", "Beta", "alpha");
  }
}

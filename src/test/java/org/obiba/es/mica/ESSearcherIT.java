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
import org.obiba.mica.spi.search.Searcher.DocumentResults;
import org.springframework.data.domain.Persistable;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class ESSearcherIT extends AbstractOpenSearchIT {

  private static final String INDEX = "it_search_studies";

  @BeforeClass
  public static void indexDocuments() throws Exception {
    setup();
    List<TestDocument> docs = List.of(
        new TestDocument("study-1", "Alpha Cohort", "cohort", 25),
        new TestDocument("study-2", "Beta Clinical", "clinical", 40),
        new TestDocument("study-3", "Gamma Cohort", "cohort", 55),
        new TestDocument("study-4", "Delta Clinical", "clinical", 70)
    );
    indexer.indexAll(INDEX, docs, null);
    Thread.sleep(1500); // let OpenSearch refresh
  }

  @Test
  public void test_find_all() {
    DocumentResults results = searcher.find(INDEX, "study", "", null);
    assertThat(results.getTotal()).isEqualTo(4);
  }

  @Test
  public void test_find_by_term() {
    DocumentResults results = searcher.find(INDEX, "study", "study(eq(type,cohort))", null);
    assertThat(results.getTotal()).isEqualTo(2);
  }

  @Test
  public void test_find_by_range() {
    // ageMin >= 50
    DocumentResults results = searcher.find(INDEX, "study", "study(ge(ageMin,50))", null);
    assertThat(results.getTotal()).isEqualTo(2);
  }

  @Test
  public void test_count_all() {
    DocumentResults results = searcher.count(INDEX, "study", "", null);
    assertThat(results.getTotal()).isEqualTo(4);
  }

  @Test
  public void test_count_by_term() {
    DocumentResults results = searcher.count(INDEX, "study", "study(eq(type,clinical))", null);
    assertThat(results.getTotal()).isEqualTo(2);
  }

  @Test
  public void test_find_with_limit_and_sort() {
    DocumentResults results = searcher.find(INDEX, "study", "study(limit(0,2),sort(ageMin))", null);
    assertThat(results.getTotal()).isEqualTo(4); // total hits
    assertThat(results.getDocuments()).hasSize(2); // page size
  }

  @Test
  public void test_find_by_in() {
    // type in (cohort, clinical) = all 4
    DocumentResults results = searcher.find(INDEX, "study", "study(in(type,(cohort,clinical)))", null);
    assertThat(results.getTotal()).isEqualTo(4);
  }

  @Test
  public void test_find_by_lt() {
    // ageMin < 50 = study-1 (25) and study-2 (40)
    DocumentResults results = searcher.find(INDEX, "study", "study(lt(ageMin,50))", null);
    assertThat(results.getTotal()).isEqualTo(2);
  }

  @Test
  public void test_find_by_le() {
    // ageMin <= 40 = study-1 (25) and study-2 (40)
    DocumentResults results = searcher.find(INDEX, "study", "study(le(ageMin,40))", null);
    assertThat(results.getTotal()).isEqualTo(2);
  }

  @Test
  public void test_find_not_term() {
    // not cohort = 2 clinical
    DocumentResults results = searcher.find(INDEX, "study", "study(not(eq(type,cohort)))", null);
    assertThat(results.getTotal()).isEqualTo(2);
  }

  @Test
  public void test_find_combined_and() {
    // type=cohort AND ageMin >= 50 = study-3 only
    DocumentResults results = searcher.find(INDEX, "study", "study(and(eq(type,cohort),ge(ageMin,50)))", null);
    assertThat(results.getTotal()).isEqualTo(1);
  }

  static class TestDocument implements Persistable<String> {
    private final String id;
    public final String name;
    public final String type;
    public final int ageMin;

    TestDocument(String id, String name, String type, int ageMin) {
      this.id = id;
      this.name = name;
      this.type = type;
      this.ageMin = ageMin;
    }

    @Override
    public String getId() { return id; }

    @Override
    public boolean isNew() { return true; }
  }
}

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

import org.junit.Test;
import org.springframework.data.domain.Persistable;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class ESIndexerIT extends AbstractOpenSearchIT {

  private static final String INDEX = "it_studies";

  @Test
  public void test_index_single_document() throws Exception {
    indexer.index(INDEX, new TestDocument("study-1", "Study One", "cohort"), null);
    Thread.sleep(1000);

    assertThat(indexer.hasIndex(INDEX)).isTrue();
  }

  @Test
  public void test_index_all_documents() throws Exception {
    List<TestDocument> docs = List.of(
        new TestDocument("study-2", "Study Two", "cohort"),
        new TestDocument("study-3", "Study Three", "clinical"),
        new TestDocument("study-4", "Study Four", "clinical")
    );
    indexer.indexAll(INDEX, docs, null);
    Thread.sleep(1000);

    assertThat(indexer.hasIndex(INDEX)).isTrue();
  }

  @Test
  public void test_drop_index() throws Exception {
    indexer.index(INDEX + "_drop", new TestDocument("study-drop", "To Drop", "cohort"), null);
    Thread.sleep(500);

    indexer.dropIndex(INDEX + "_drop");

    assertThat(indexer.hasIndex(INDEX + "_drop")).isFalse();
  }

  @Test
  public void test_delete_document() throws Exception {
    String deleteIndex = INDEX + "_delete";
    indexer.index(deleteIndex, new TestDocument("study-del", "To Delete", "cohort"), null);
    Thread.sleep(500);

    indexer.delete(deleteIndex, new TestDocument("study-del", null, null));
    Thread.sleep(500);

    assertThat(indexer.hasIndex(deleteIndex)).isTrue();
  }

  static class TestDocument implements Persistable<String> {
    private final String id;
    public final String name;
    public final String type;

    TestDocument(String id, String name, String type) {
      this.id = id;
      this.name = name;
      this.type = type;
    }

    @Override
    public String getId() { return id; }

    @Override
    public boolean isNew() { return true; }
  }
}

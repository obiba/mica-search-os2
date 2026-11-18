/*
 * Copyright (c) 2026 OBiBa. All rights reserved.
 *
 * This program and the accompanying materials
 * are made available under the terms of the GNU Public License v3.0.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.obiba.es.mica;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import org.junit.Test;
import org.obiba.es.mica.query.RQLQuery;
import org.opensearch.client.opensearch._types.FieldSort;
import org.opensearch.client.opensearch._types.SortOptions;
import org.opensearch.client.opensearch._types.SortOrder;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

public class SortTest {

  private static final String MAPPING = """
      {
        "acronym": {"properties": {
          "en": {"type": "keyword", "fields": {"analyzed": {"type": "text"}, "sort": {"type": "keyword"}}}
        }},
        "description": {"properties": {
          "en": {"type": "keyword", "fields": {"analyzed": {"type": "text"}}}
        }},
        "id": {"type": "keyword"}
      }
      """;

  private static ESIndexer.IndexFieldMappingImpl mapping(boolean caseInsensitive) {
    return new ESIndexer.IndexFieldMappingImpl(
        JsonPath.using(Configuration.defaultConfiguration().addOptions(Option.ALWAYS_RETURN_LIST)).parse(MAPPING),
        caseInsensitive);
  }

  @Test
  public void test_is_sortable() {
    ESIndexer.IndexFieldMappingImpl mapping = mapping(true);
    assertThat(mapping.isSortable("acronym.en")).isTrue();
    assertThat(mapping.isSortable("acronym")).isFalse();
    assertThat(mapping.isSortable("description.en")).isFalse();
    assertThat(mapping.isSortable("id")).isFalse();
    assertThat(mapping.isSortable("unknown.en")).isFalse();
    assertThat(mapping(false).isSortable("acronym.en")).isFalse();
    assertThat(new ESIndexer.IndexFieldMappingImpl(null, true).isSortable("acronym.en")).isFalse();
  }

  @Test
  public void test_sort_sub_field_then_raw_field() {
    List<FieldSort> sorts = ESSearcher.toSortOptions(new RQLQuery("study(sort(-acronym.en,id))").getSortBuilders(),
        () -> mapping(true)::isSortable).stream().map(SortOptions::field).toList();
    assertThat(sorts).extracting(FieldSort::field).containsExactly("acronym.en.sort", "acronym.en", "id");
    assertThat(sorts).extracting(FieldSort::order).containsExactly(SortOrder.Desc, SortOrder.Desc, SortOrder.Asc);
  }

  @Test
  public void test_mapping_not_fetched_for_score() {
    AtomicInteger calls = new AtomicInteger();
    ESSearcher.toSortOptions(new RQLQuery("study(sort(_score))").getSortBuilders(), () -> {
      calls.incrementAndGet();
      return f -> true;
    });
    assertThat(calls.get()).isZero();
  }
}

/*
 * Copyright (c) 2026 OBiBa. All rights reserved.
 *
 * This program and the accompanying materials
 * are made available under the terms of the GNU Public License v3.0.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.obiba.es.mica.query;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.obiba.es.mica.OSQuery;
import org.obiba.mica.spi.search.support.Query;

import org.opensearch.client.opensearch._types.query_dsl.BoolQuery;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Combines several queries with and/must.
 */
public class AndQuery implements OSQuery {

  private final List<OSQuery> queries;

  public AndQuery(Query... queries) {
    this.queries = Arrays.stream(queries).filter(q -> !q.isEmpty())
        .map(q -> (OSQuery) q).collect(Collectors.toList());
  }

  @Override
  public int getFrom() {
    return queries.stream().filter(OSQuery::hasLimit).map(OSQuery::getFrom).findFirst().orElse(0);
  }

  @Override
  public int getSize() {
    return queries.stream().filter(OSQuery::hasLimit).map(OSQuery::getSize).findFirst().orElse(0);
  }

  @Override
  public boolean hasLimit() {
    return queries.stream().anyMatch(OSQuery::hasLimit);
  }

  @Override
  public boolean hasQueryBuilder() {
    return queries.stream().anyMatch(OSQuery::hasQueryBuilder);
  }

  @Override
  public org.opensearch.client.opensearch._types.query_dsl.Query getQueryBuilder() {

    BoolQuery.Builder builder = new BoolQuery.Builder();
    queries.stream().filter(OSQuery::hasQueryBuilder).forEach(q -> builder.must(q.getQueryBuilder()));
    return builder.build()._toQuery();
  }

  @Override
  public boolean hasSortBuilders() {
    return queries.stream().anyMatch(OSQuery::hasSortBuilders);
  }

  @Override
  public List<Map.Entry<String, String>> getSortBuilders() {
    List<Map.Entry<String, String>> sorts = Lists.newArrayList();
    queries.stream().filter(OSQuery::hasSortBuilders).forEach(q -> sorts.addAll(q.getSortBuilders()));
    return sorts;
  }

  @Override
  public List<String> getSourceFields() {
    List<String> fields = Lists.newArrayList();
    queries.forEach(q -> fields.addAll(q.getSourceFields()));
    return fields;
  }

  @Override
  public List<String> getAggregationBuckets() {
    List<String> buckets = Lists.newArrayList();
    queries.forEach(q -> buckets.addAll(q.getAggregationBuckets()));
    return buckets;
  }

  @Override
  public List<String> getQueryAggregationBuckets() {
    return Lists.newArrayList();
  }

  @Override
  public void ensureAggregationBuckets(List<String> additionalAggregationBuckets) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> getAggregations() {
    List<String> aggs = Lists.newArrayList();
    queries.forEach(q -> aggs.addAll(q.getAggregations()));
    return aggs;
  }

  @Override
  public Map<String, Map<String, List<String>>> getTaxonomyTermsMap() {
    return Maps.newHashMap();
  }

  @Override
  public boolean isEmpty() {
    return queries.stream().allMatch(Query::isEmpty);
  }

  @Override
  public boolean hasIdCriteria() {
    return queries.stream().anyMatch(Query::hasIdCriteria);
  }
}

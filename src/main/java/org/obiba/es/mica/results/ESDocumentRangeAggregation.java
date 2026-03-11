/*
 * Copyright (c) 2026 OBiBa. All rights reserved.
 *
 * This program and the accompanying materials
 * are made available under the terms of the GNU Public License v3.0.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.obiba.es.mica.results;

import org.opensearch.client.opensearch._types.aggregations.Aggregate;
import org.opensearch.client.opensearch._types.aggregations.RangeAggregate;

import org.obiba.mica.spi.search.Searcher;

import java.util.List;
import java.util.stream.Collectors;

/**
 * {@link RangeAggregate} aggregation wrapper.
 */
public class ESDocumentRangeAggregation implements Searcher.DocumentRangeAggregation {
  private final RangeAggregate range;

  public ESDocumentRangeAggregation(Aggregate range) {
    this.range = range.range();
  }

  @Override
  public List<Searcher.DocumentRangeBucket> getBuckets() {
    return range.buckets().array().stream().map(ESDocumentRangeBucket::new).collect(Collectors.toList());
  }
}

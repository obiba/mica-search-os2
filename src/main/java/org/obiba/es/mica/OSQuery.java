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

import org.obiba.mica.spi.search.support.Query;

import java.util.List;
import java.util.Map;

public interface OSQuery extends Query {

  boolean hasLimit();

  org.opensearch.client.opensearch._types.query_dsl.Query getQueryBuilder();

  boolean hasSortBuilders();

  /**
   * Returns sort specifications as a list of field→order pairs ("Asc" or "Desc").
   */
  List<Map.Entry<String, String>> getSortBuilders();

}

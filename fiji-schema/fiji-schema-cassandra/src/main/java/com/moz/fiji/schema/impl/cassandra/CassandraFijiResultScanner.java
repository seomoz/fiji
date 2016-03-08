/**
 * (c) Copyright 2014 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.moz.fiji.schema.impl.cassandra;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import org.mortbay.io.RuntimeIOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.commons.IteratorUtils;
import com.moz.fiji.schema.EntityId;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiDataRequest.Column;
import com.moz.fiji.schema.FijiResult;
import com.moz.fiji.schema.FijiResultScanner;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.avro.RowKeyFormat2;
import com.moz.fiji.schema.cassandra.CassandraTableName;
import com.moz.fiji.schema.impl.cassandra.RowDecoders.TokenRowKeyComponents;
import com.moz.fiji.schema.impl.cassandra.RowDecoders.TokenRowKeyComponentsComparator;
import com.moz.fiji.schema.layout.CassandraColumnNameTranslator;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.layout.impl.CellDecoderProvider;
import com.moz.fiji.schema.layout.impl.ColumnId;

/**
 * {@inheritDoc}
 *
 * Cassandra implementation of {@code FijiResultScanner}.
 *
 * @param <T> type of {@code FijiCell} value returned by scanned {@code FijiResult}s.
 */
public class CassandraFijiResultScanner<T> implements FijiResultScanner<T> {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraFijiResultScanner.class);
  private final Iterator<FijiResult<T>> mIterator;

 /*
  * ## Implementation Notes
  *
  * To perform a scan over a Fiji table, Cassandra Fiji creates an entityID scan over all locality
  * group tables in the scan, and then for each entity Id, creates a separate FijiResult. Creating
  * each Fiji result requires more requests to create the paged and non-paged columns.
  *
  * This has the downside of creating multiple separate CQL queries per Fiji row instead of a
  * finite number of scans which work through the data. This is the simplest way to implement
  * scanning at this point in time, but it may be possible in the future to use CQL scans.  CQL
  * scans have many downsides, though; often ALLOW FILTERING clause is needed, and we have
  * experienced poor performance and timeouts when using it.
  *
  * TODO: we could optimize this by using table scans to pull in the materialized rows as part of a
  * scan instead of issuing separate get requests for each row.  However, it is not clear that this
  * will be faster given the idiosyncrasies of Cassandra scans.
  */

  /**
   * Create a {@link FijiResultScanner} over a Cassandra Fiji table with the provided options.
   *
   * @param request The data request defining the columns to scan.
   * @param tokenRange The range of tokens to scan.
   * @param table The table to scan.
   * @param layout The layout of the table.
   * @param decoderProvider A cell decoder provider for the table.
   * @param translator A column name translator for the table.
   * @throws IOException On unrecoverable IO error.
   */
  public CassandraFijiResultScanner(
      final FijiDataRequest request,
      final Range<Long> tokenRange,
      final CassandraFijiTable table,
      final FijiTableLayout layout,
      final CellDecoderProvider decoderProvider,
      final CassandraColumnNameTranslator translator

  ) throws IOException {

    final Set<ColumnId> localityGroups = Sets.newHashSet();

    for (final Column columnRequest : request.getColumns()) {
      final ColumnId localityGroupId =
          layout.getFamilyMap().get(columnRequest.getFamily()).getLocalityGroup().getId();
      localityGroups.add(localityGroupId);
    }

    final FijiURI tableURI = table.getURI();
    final List<CassandraTableName> tableNames = Lists.newArrayList();
    for (final ColumnId localityGroup : localityGroups) {
      final CassandraTableName tableName =
          CassandraTableName.getLocalityGroupTableName(tableURI, localityGroup);
      tableNames.add(tableName);
    }

    mIterator = Iterators.transform(
        getEntityIDs(tableNames, tokenRange, table, layout),
        new Function<EntityId, FijiResult<T>>() {
          /** {@inheritDoc} */
          @Override
          public FijiResult<T> apply(final EntityId entityId) {
            try {
              return CassandraFijiResult.create(
                  entityId,
                  request,
                  table,
                  layout,
                  translator,
                  decoderProvider);
            } catch (IOException e) {
              throw new RuntimeIOException(e);
            }
          }
        });
  }

  /**
   * Get an iterator of the entity IDs in a list of Cassandra Fiji tables that correspond to a
   * subset of cassandra tables in a Fiji table.
   *
   * @param tables The Cassandra tables to get Entity IDs from.
   * @param tokenRange The token range to scan.
   * @param table The Fiji Cassandra table which the Cassandra tables belong to.
   * @param layout The layout of the Fiji Cassandra table.
   * @return An iterator of Entity IDs.
   */
  public static Iterator<EntityId> getEntityIDs(
      final List<CassandraTableName> tables,
      final Range<Long> tokenRange,
      final CassandraFijiTable table,
      final FijiTableLayout layout
  ) {

    final CQLStatementCache statementCache = table.getStatementCache();
    final List<ResultSetFuture> localityGroupFutures =
        FluentIterable
            .from(tables)
            .transform(
                new Function<CassandraTableName, Statement>() {
                  /** {@inheritDoc} */
                  @Override
                  public Statement apply(final CassandraTableName tableName) {
                    return statementCache.createEntityIDScanStatement(tableName, tokenRange);
                  }
                })
            .transform(
                new Function<Statement, ResultSetFuture>() {
                  /** {@inheritDoc} */
                  @Override
                  public ResultSetFuture apply(final Statement statement) {
                    return table.getAdmin().executeAsync(statement);
                  }
                })
            // Force futures to execute by sending results to a list
            .toList();

    // We can use the DISTINCT optimization iff the entity ID contains only hashed components
    RowKeyFormat2 keyFormat = (RowKeyFormat2) layout.getDesc().getKeysFormat();
    final boolean deduplicateComponents =
        keyFormat.getRangeScanStartIndex() != keyFormat.getComponents().size();

    if (deduplicateComponents) {
      LOG.warn("Scanning a Cassandra Fiji table with non-hashed entity ID components is"
              + " inefficient.  Consider hashing all entity ID components. Table: {}.",
          table.getURI());
    }

    final List<Iterator<TokenRowKeyComponents>> tokenRowKeyStreams =
        FluentIterable
            .from(localityGroupFutures)
            .transform(
                new Function<ResultSetFuture, Iterator<Row>>() {
                  /** {@inheritDoc} */
                  @Override
                  public Iterator<Row> apply(final ResultSetFuture future) {
                    return CassandraFijiResult.unwrapFuture(future).iterator();
                  }
                })
            .transform(
                new Function<Iterator<Row>, Iterator<TokenRowKeyComponents>>() {
                  /** {@inheritDoc} */
                  @Override
                  public Iterator<TokenRowKeyComponents> apply(final Iterator<Row> rows) {
                    return Iterators.transform(rows, RowDecoders.getRowKeyDecoderFunction(layout));
                  }
                })
            .transform(
                new Function<Iterator<TokenRowKeyComponents>, Iterator<TokenRowKeyComponents>>() {
                  /** {@inheritDoc} */
                  @Override
                  public Iterator<TokenRowKeyComponents> apply(
                      final Iterator<TokenRowKeyComponents> components
                  ) {
                    if (deduplicateComponents) {
                      return IteratorUtils.deduplicatingIterator(components);
                    } else {
                      return components;
                    }
                  }
                })
            .toList();

    return
        Iterators.transform(
            IteratorUtils.deduplicatingIterator(
                Iterators.mergeSorted(
                    tokenRowKeyStreams,
                    TokenRowKeyComponentsComparator.getInstance())),
            RowDecoders.getEntityIdFunction(table));
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public boolean hasNext() {
    return mIterator.hasNext();
  }

  @Override
  public FijiResult<T> next() {
    return mIterator.next();
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
}

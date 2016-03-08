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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.commons.ResourceTracker;
import com.moz.fiji.schema.EntityId;
import com.moz.fiji.schema.FijiColumnName;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiDataRequestValidator;
import com.moz.fiji.schema.FijiPartition;
import com.moz.fiji.schema.FijiResult;
import com.moz.fiji.schema.FijiResultScanner;
import com.moz.fiji.schema.FijiRowData;
import com.moz.fiji.schema.FijiRowScanner;
import com.moz.fiji.schema.FijiTableReader;
import com.moz.fiji.schema.FijiTableReaderBuilder;
import com.moz.fiji.schema.FijiTableReaderBuilder.OnDecoderCacheMiss;
import com.moz.fiji.schema.NoSuchColumnException;
import com.moz.fiji.schema.SpecificCellDecoderFactory;
import com.moz.fiji.schema.impl.BoundColumnReaderSpec;
import com.moz.fiji.schema.impl.FijiResultRowData;
import com.moz.fiji.schema.impl.FijiResultRowScanner;
import com.moz.fiji.schema.impl.LayoutConsumer;
import com.moz.fiji.schema.layout.CassandraColumnNameTranslator;
import com.moz.fiji.schema.layout.CellSpec;
import com.moz.fiji.schema.layout.ColumnReaderSpec;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.layout.impl.CellDecoderProvider;

/**
 * Reads from a fiji table by sending the requests directly to the C* tables.
 */
@ApiAudience.Private
public final class CassandraFijiTableReader implements FijiTableReader {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraFijiTableReader.class);

  /** C* FijiTable to read from. */
  private final CassandraFijiTable mTable;

  /** Behavior when a cell decoder cannot be found. */
  private final OnDecoderCacheMiss mOnDecoderCacheMiss;

  /** States of a fiji table reader instance. */
  private static enum State {
    UNINITIALIZED,
    OPEN,
    CLOSED
  }

  /** Tracks the state of this FijiTableReader instance. */
  private final AtomicReference<State> mState = new AtomicReference<State>(State.UNINITIALIZED);

  /** Map of overridden CellSpecs to use when reading. Null when mOverrides is not null. */
  private final Map<FijiColumnName, CellSpec> mCellSpecOverrides;

  /** Map of overridden column read specifications. Null when mCellSpecOverrides is not null. */
  private final Map<FijiColumnName, BoundColumnReaderSpec> mOverrides;

  /** Map of backup column read specifications. Null when mCellSpecOverrides is not null. */
  private final Collection<BoundColumnReaderSpec> mAlternatives;

  /** Layout consumer registration resource. */
  private final LayoutConsumer.Registration mLayoutConsumerRegistration;

  /** Object which processes layout update from the FijiTable from which this Reader reads. */
  private final InnerLayoutUpdater mInnerLayoutUpdater = new InnerLayoutUpdater();

  /**
   * Encapsulation of all table layout related state necessary for the operation of this reader.
   * Can be hot swapped to reflect a table layout update.
   */
  private ReaderLayoutCapsule mReaderLayoutCapsule = null;

  /**
   * Container class encapsulating all reader state which must be updated in response to a table
   * layout update.
   */
  private static final class ReaderLayoutCapsule {
    private final CellDecoderProvider mCellDecoderProvider;
    private final FijiTableLayout mLayout;
    private final CassandraColumnNameTranslator mTranslator;

    /**
     * Default constructor.
     *
     * @param cellDecoderProvider the CellDecoderProvider to cache.  This provider should reflect
     *     all overrides appropriate to this reader.
     * @param layout the FijiTableLayout to cache.
     * @param translator the ColumnNameTranslator to cache.
     */
    private ReaderLayoutCapsule(
        final CellDecoderProvider cellDecoderProvider,
        final FijiTableLayout layout,
        final CassandraColumnNameTranslator translator) {
      mCellDecoderProvider = cellDecoderProvider;
      mLayout = layout;
      mTranslator = translator;
    }

    /**
     * Get the column name translator for the current layout.
     * @return the column name translator for the current layout.
     */
    private CassandraColumnNameTranslator getColumnNameTranslator() {
      return mTranslator;
    }

    /**
     * Get the current table layout for the table to which this reader is associated.
     * @return the current table layout for the table to which this reader is associated.
     */
    private FijiTableLayout getLayout() {
      return mLayout;
    }

    /**
     * Get the CellDecoderProvider including CellSpec overrides for providing cell decoders for the
     * current layout.
     * @return the CellDecoderProvider including CellSpec overrides for providing cell decoders for
     * the current layout.
     */
    private CellDecoderProvider getCellDecoderProvider() {
      return mCellDecoderProvider;
    }
  }

  /** Provides for the updating of this Reader in response to a table layout update. */
  private final class InnerLayoutUpdater implements LayoutConsumer {
    /** {@inheritDoc} */
    @Override
    public void update(final FijiTableLayout layout) throws IOException {
      final CellDecoderProvider provider;
      if (null != mCellSpecOverrides) {
        provider = CellDecoderProvider.create(
            layout,
            mTable.getFiji().getSchemaTable(),
            SpecificCellDecoderFactory.get(),
            mCellSpecOverrides);
      } else {
        provider = CellDecoderProvider.create(
            layout,
            mOverrides,
            mAlternatives,
            mOnDecoderCacheMiss);
      }
      if (mReaderLayoutCapsule != null) {
        LOG.debug(
            "Updating layout used by FijiTableReader: {} for table: {} from version: {} to: {}",
            this,
            mTable.getURI(),
            mReaderLayoutCapsule.getLayout().getDesc().getLayoutId(),
            layout.getDesc().getLayoutId());
      } else {
        // If the capsule is null this is the initial setup and we need a different log message.
        LOG.debug(
            "Initializing FijiTableReader: {} for table: {} with table layout version: {}",
            this,
            mTable.getURI(),
            layout.getDesc().getLayoutId());
      }
      mReaderLayoutCapsule =
          new ReaderLayoutCapsule(provider, layout, CassandraColumnNameTranslator.from(layout));
    }
  }

  /**
   * Creates a new {@code CassandraFijiTableReader} instance that sends the read requests
   * directly to Cassandra.
   *
   * @param table Fiji table from which to read.
   * @throws java.io.IOException on I/O error.
   * @return a new CassandraFijiTableReader.
   */
  public static CassandraFijiTableReader create(
      final CassandraFijiTable table
  ) throws IOException {
    return CassandraFijiTableReaderBuilder.create(table).buildAndOpen();
  }

  /**
   * Creates a new CassandraFijiTableReader instance that sends read requests directly to Cassandra.
   *
   * @param table Fiji table from which to read.
   * @param overrides layout overrides to modify read behavior.
   * @return a new CassandraFijiTableReader.
   * @throws java.io.IOException in case of an error opening the reader.
   */
  public static CassandraFijiTableReader createWithCellSpecOverrides(
      final CassandraFijiTable table,
      final Map<FijiColumnName, CellSpec> overrides
  ) throws IOException {
    return new CassandraFijiTableReader(table, overrides);
  }

  /**
   * Creates a new CassandraFijiTableReader instance that sends read requests directly to Cassandra.
   *
   * @param table Fiji table from which to read.
   * @param onDecoderCacheMiss behavior to use when a {@link
   *     com.moz.fiji.schema.layout.ColumnReaderSpec} override specified in a {@link
   *     com.moz.fiji.schema.FijiDataRequest} cannot be found in the prebuilt cache of cell decoders.
   * @param overrides mapping from columns to overriding read behavior for those columns.
   * @param alternatives mapping from columns to reader spec alternatives which the
   *     FijiTableReader will accept as overrides in data requests.
   * @return a new CassandraFijiTableReader.
   * @throws java.io.IOException in case of an error opening the reader.
   */
  public static CassandraFijiTableReader createWithOptions(
      final CassandraFijiTable table,
      final OnDecoderCacheMiss onDecoderCacheMiss,
      final Map<FijiColumnName, ColumnReaderSpec> overrides,
      final Multimap<FijiColumnName, ColumnReaderSpec> alternatives
  ) throws IOException {
    return new CassandraFijiTableReader(table, onDecoderCacheMiss, overrides, alternatives);
  }

  /**
   * Open a table reader whose behavior is customized by overriding CellSpecs.
   *
   * @param table Fiji table from which this reader will read.
   * @param cellSpecOverrides specifications of overriding read behaviors.
   * @throws java.io.IOException in case of an error opening the reader.
   */
  private CassandraFijiTableReader(
      final CassandraFijiTable table,
      final Map<FijiColumnName, CellSpec> cellSpecOverrides
  ) throws IOException {
    mTable = table;
    mCellSpecOverrides = cellSpecOverrides;
    mOnDecoderCacheMiss = FijiTableReaderBuilder.DEFAULT_CACHE_MISS;
    mOverrides = null;
    mAlternatives = null;

    mLayoutConsumerRegistration = mTable.registerLayoutConsumer(mInnerLayoutUpdater);
    Preconditions.checkState(mReaderLayoutCapsule != null,
        "FijiTableReader for table: %s failed to initialize.", mTable.getURI());

    // Retain the table only when everything succeeds.
    mTable.retain();
    final State oldState = mState.getAndSet(State.OPEN);
    Preconditions.checkState(oldState == State.UNINITIALIZED,
        "Cannot open FijiTableReader instance in state %s.", oldState);
    ResourceTracker.get().registerResource(this);
  }

  /**
   * Creates a new CassandraFijiTableReader instance that sends read requests directly to Cassandra.
   *
   * @param table Fiji table from which to read.
   * @param onDecoderCacheMiss behavior to use when a {@link
   *     com.moz.fiji.schema.layout.ColumnReaderSpec} override specified in a {@link
   *     com.moz.fiji.schema.FijiDataRequest} cannot be found in the prebuilt cache of cell decoders.
   * @param overrides mapping from columns to overriding read behavior for those columns.
   * @param alternatives mapping from columns to reader spec alternatives which the
   *     FijiTableReader will accept as overrides in data requests.
   * @throws java.io.IOException on I/O error.
   */
  private CassandraFijiTableReader(
      final CassandraFijiTable table,
      final OnDecoderCacheMiss onDecoderCacheMiss,
      final Map<FijiColumnName, ColumnReaderSpec> overrides,
      final Multimap<FijiColumnName, ColumnReaderSpec> alternatives
  ) throws IOException {
    mTable = table;
    mOnDecoderCacheMiss = onDecoderCacheMiss;

    final FijiTableLayout layout = mTable.getLayout();
    final Set<FijiColumnName> layoutColumns = layout.getColumnNames();
    final Map<FijiColumnName, BoundColumnReaderSpec> boundOverrides = Maps.newHashMap();
    for (Map.Entry<FijiColumnName, ColumnReaderSpec> override
        : overrides.entrySet()) {
      final FijiColumnName column = override.getKey();
      if (!layoutColumns.contains(column)
          && !layoutColumns.contains(new FijiColumnName(column.getFamily()))) {
        throw new NoSuchColumnException(String.format(
            "FijiTableLayout: %s does not contain column: %s", layout, column));
      } else {
        boundOverrides.put(column,
            BoundColumnReaderSpec.create(override.getValue(), column));
      }
    }
    mOverrides = boundOverrides;
    final Collection<BoundColumnReaderSpec> boundAlternatives = Sets.newHashSet();
    for (Map.Entry<FijiColumnName, ColumnReaderSpec> altsEntry
        : alternatives.entries()) {
      final FijiColumnName column = altsEntry.getKey();
      if (!layoutColumns.contains(column)
          && !layoutColumns.contains(FijiColumnName.create(column.getFamily()))) {
        throw new NoSuchColumnException(String.format(
            "FijiTableLayout: %s does not contain column: %s", layout, column));
      } else {
        boundAlternatives.add(
            BoundColumnReaderSpec.create(altsEntry.getValue(), altsEntry.getKey()));
      }
    }
    mAlternatives = boundAlternatives;
    mCellSpecOverrides = null;

    mLayoutConsumerRegistration = mTable.registerLayoutConsumer(mInnerLayoutUpdater);
    Preconditions.checkState(mReaderLayoutCapsule != null,
        "FijiTableReader for table: %s failed to initialize.", mTable.getURI());

    // Retain the table only when everything succeeds.
    mTable.retain();
    final State oldState = mState.getAndSet(State.OPEN);
    Preconditions.checkState(oldState == State.UNINITIALIZED,
        "Cannot open FijiTableReader instance in state %s.", oldState);
    ResourceTracker.get().registerResource(this);
  }

  /** {@inheritDoc} */
  @Override
  public FijiRowData get(
      final EntityId entityId,
      final FijiDataRequest dataRequest
  ) throws IOException {

    return new FijiResultRowData(
        mReaderLayoutCapsule.getLayout(),
        getResult(entityId, dataRequest));
  }

  /**
   * Get a FijiResult for the given EntityId and data request.
   *
   * <p>
   *   This method allows the caller to specify a type-bound on the values of the {@code FijiCell}s
   *   of the returned {@code FijiResult}. The caller should be careful to only specify an
   *   appropriate type. If the type is too specific (or wrong), a runtime
   *   {@link java.lang.ClassCastException} will be thrown when the returned {@code FijiResult} is
   *   used. See the 'Type Safety' section of {@link FijiResult}'s documentation for more details.
   * </p>
   *
   * @param entityId EntityId of the row from which to get data.
   * @param dataRequest Specification of the data to get from the given row.
   * @param <T> type {@code FijiCell} value returned by the {@code FijiResult}.
   * @return a new FijiResult for the given EntityId and data request.
   * @throws IOException in case of an error getting the data.
   */
  @Override
  public <T> FijiResult<T> getResult(
      final EntityId entityId,
      final FijiDataRequest dataRequest
  ) throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get row from FijiTableReader instance %s in state %s.", this, state);

    final ReaderLayoutCapsule capsule = mReaderLayoutCapsule;
    // Make sure the request validates against the layout of the table.
    final FijiTableLayout tableLayout = capsule.getLayout();
    validateRequestAgainstLayout(dataRequest, tableLayout);

    return CassandraFijiResult.create(
        entityId,
        dataRequest,
        mTable,
        tableLayout,
        capsule.getColumnNameTranslator(),
        capsule.getCellDecoderProvider());
  }


  /** {@inheritDoc} */
  @Override
  public List<FijiRowData> bulkGet(
      final List<EntityId> entityIds,
      final FijiDataRequest dataRequest
  ) throws IOException {
    // TODO(SCHEMA-981): make this use async requests
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get rows from FijiTableReader instance %s in state %s.", this, state);
    List<FijiRowData> data = Lists.newArrayList();
    for (EntityId eid : entityIds) {
      data.add(get(eid, dataRequest));
    }
    return data;
  }

  /** {@inheritDoc} */
  @Override
  public <T> List<FijiResult<T>> bulkGetResults(
      final List<EntityId> entityIds,
      final FijiDataRequest dataRequest
  ) throws IOException {
    // TODO(SCHEMA-981): make this use async requests
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get rows from FijiTableReader instance %s in state %s.", this, state);
    List<FijiResult<T>> data = Lists.newArrayList();
    for (EntityId eid : entityIds) {
      data.add(this.<T>getResult(eid, dataRequest));
    }
    return data;
  }

  /** {@inheritDoc} */
  @Override
  public FijiRowScanner getScanner(final FijiDataRequest dataRequest) throws IOException {
    return getScannerWithOptions(dataRequest, CassandraFijiScannerOptions.withoutBounds());
  }

  /** {@inheritDoc} */
  @Override
  public FijiRowScanner getScanner(
      final FijiDataRequest dataRequest,
      final FijiScannerOptions fijiScannerOptions
  ) throws IOException {
    throw new UnsupportedOperationException("Cassandra Fiji cannot use FijiScannerOptions.");
  }

  /**
   * Returns a new RowScanner configured with Cassandra-specific scanner options.
   *
   * @param request for the scan.
   * @param fijiScannerOptions Cassandra-specific scan options.
   * @return A new row scanner.
   * @throws IOException if there is a problem creating the row scanner.
   */
  public FijiRowScanner getScannerWithOptions(
      final FijiDataRequest request,
      final CassandraFijiScannerOptions fijiScannerOptions
  ) throws IOException {
    return new FijiResultRowScanner(
        mReaderLayoutCapsule.getLayout(),
        getFijiResultScanner(request, fijiScannerOptions));
  }

  /** {@inheritDoc} */
  @Override
  public <T> CassandraFijiResultScanner<T> getFijiResultScanner(
      final FijiDataRequest request
  ) throws IOException {
    return getFijiResultScanner(request, CassandraFijiScannerOptions.withoutBounds());
  }

  /** {@inheritDoc} */
  @Override
  public <T> FijiResultScanner<T> getFijiResultScanner(
      final FijiDataRequest dataRequest,
      final FijiPartition partition
  ) throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Can not get scanner from FijiTableReader instance %s in state %s.", this, state);

    Preconditions.checkArgument(partition instanceof CassandraFijiPartition,
        "Can not scan a Cassandra table with a non-Cassandra partition.");
    final CassandraFijiPartition cassandraPartition = (CassandraFijiPartition) partition;

    final ReaderLayoutCapsule capsule = mReaderLayoutCapsule;

    // Make sure the request validates against the layout of the table.
    final FijiTableLayout layout = capsule.getLayout();
    validateRequestAgainstLayout(dataRequest, layout);

    return new CassandraFijiResultScanner<>(
        dataRequest,
        cassandraPartition.getTokenRange(),
        mTable,
        layout,
        capsule.getCellDecoderProvider(),
        capsule.getColumnNameTranslator());
  }

  /**
   * Get a CassandraFijiResultScanner using the specified data request and
   * CassandraFijiScannerOptions.
   *
   * @param dataRequest Specifies the data to request from each row.
   * @param options Other Cassandra options for the scanner.
   * @param <T> Type of the data in the requested cells.
   * @return A FijiResultScanner for the requested data.
   * @throws IOException In case of an error reading from the table.
   */
  public <T> CassandraFijiResultScanner<T> getFijiResultScanner(
      final FijiDataRequest dataRequest,
      final CassandraFijiScannerOptions options
  ) throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get scanner from FijiTableReader instance %s in state %s.", this, state);

    final ReaderLayoutCapsule capsule = mReaderLayoutCapsule;

    // Make sure the request validates against the layout of the table.
    final FijiTableLayout layout = capsule.getLayout();
    validateRequestAgainstLayout(dataRequest, layout);

    final Range<Long> tokenRange;
    if (options.hasStartToken() && options.hasStopToken()) {
      tokenRange = Range.closedOpen(options.getStartToken(), options.getStopToken());
    } else if (options.hasStartToken()) {
      tokenRange = Range.atLeast(options.getStartToken());
    } else if (options.hasStopToken()) {
      tokenRange = Range.lessThan(options.getStopToken());
    } else {
      tokenRange = Range.all();
    }

    return new CassandraFijiResultScanner<>(
        dataRequest,
        tokenRange,
        mTable,
        layout,
        capsule.getCellDecoderProvider(),
        capsule.getColumnNameTranslator());
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(CassandraFijiTableReader.class)
        .add("id", System.identityHashCode(this))
        .add("table", mTable.getURI())
        .add("layout-version", mReaderLayoutCapsule.getLayout().getDesc().getLayoutId())
        .add("state", mState.get())
        .toString();
  }

  /**
   * Validate a data request against a table layout.
   *
   * @param dataRequest A FijiDataRequest.
   * @param layout the FijiTableLayout of the table against which to validate the data request.
   */
  private void validateRequestAgainstLayout(FijiDataRequest dataRequest, FijiTableLayout layout) {
    // TODO(SCHEMA-263): This could be made more efficient if the layout and/or validator were
    // cached.
    FijiDataRequestValidator.validatorForLayout(layout).validate(dataRequest);
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    final State oldState = mState.getAndSet(State.CLOSED);
    Preconditions.checkState(oldState == State.OPEN,
        "Cannot close FijiTableReader instance %s in state %s.", this, oldState);
    mLayoutConsumerRegistration.close();
    mTable.release();
    ResourceTracker.get().unregisterResource(this);
  }
}

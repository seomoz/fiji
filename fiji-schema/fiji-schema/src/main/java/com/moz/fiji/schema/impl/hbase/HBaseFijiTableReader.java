/**
 * (c) Copyright 2012 WibiData, Inc.
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

package com.moz.fiji.schema.impl.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.commons.ResourceTracker;
import com.moz.fiji.schema.EntityId;
import com.moz.fiji.schema.HBaseEntityId;
import com.moz.fiji.schema.InternalFijiError;
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
import com.moz.fiji.schema.filter.FijiRowFilter;
import com.moz.fiji.schema.filter.FijiRowFilterApplicator;
import com.moz.fiji.schema.hbase.HBaseScanOptions;
import com.moz.fiji.schema.impl.BoundColumnReaderSpec;
import com.moz.fiji.schema.impl.LayoutConsumer;
import com.moz.fiji.schema.layout.CellSpec;
import com.moz.fiji.schema.layout.ColumnReaderSpec;
import com.moz.fiji.schema.layout.HBaseColumnNameTranslator;
import com.moz.fiji.schema.layout.InvalidLayoutException;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.layout.impl.CellDecoderProvider;

/**
 * Reads from a fiji table by sending the requests directly to the HBase tables.
 */
@ApiAudience.Private
public final class HBaseFijiTableReader implements FijiTableReader {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseFijiTableReader.class);

  /** HBase FijiTable to read from. */
  private final HBaseFijiTable mTable;
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
    private final HBaseColumnNameTranslator mTranslator;

    /**
     * Default constructor.
     *
     * @param cellDecoderProvider the CellDecoderProvider to cache.  This provider should reflect
     *     all overrides appropriate to this reader.
     * @param layout the FijiTableLayout to cache.
     * @param translator the FijiColumnNameTranslator to cache.
     */
    private ReaderLayoutCapsule(
        final CellDecoderProvider cellDecoderProvider,
        final FijiTableLayout layout,
        final HBaseColumnNameTranslator translator) {
      mCellDecoderProvider = cellDecoderProvider;
      mLayout = layout;
      mTranslator = translator;
    }

    /**
     * Get the column name translator for the current layout.
     * @return the column name translator for the current layout.
     */
    private HBaseColumnNameTranslator getColumnNameTranslator() {
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
    public void update(FijiTableLayout layout) throws IOException {
      if (mState.get() == State.CLOSED) {
        LOG.debug("FijiTableReader instance is closed; ignoring layout update.");
        return;
      }
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
        LOG.debug("Initializing FijiTableReader: {} for table: {} with table layout version: {}",
            this,
            mTable.getURI(),
            layout.getDesc().getLayoutId());
      }
      mReaderLayoutCapsule = new ReaderLayoutCapsule(
          provider,
          layout,
          HBaseColumnNameTranslator.from(layout));
    }
  }

  /**
   * Creates a new <code>HBaseFijiTableReader</code> instance that sends the read requests
   * directly to HBase.
   *
   * @param table Fiji table from which to read.
   * @throws IOException on I/O error.
   * @return a new HBaseFijiTableReader.
   */
  public static HBaseFijiTableReader create(
      final HBaseFijiTable table
  ) throws IOException {
    return HBaseFijiTableReaderBuilder.create(table).buildAndOpen();
  }

  /**
   * Creates a new <code>HbaseFijiTableReader</code> instance that sends read requests directly to
   * HBase.
   *
   * @param table Fiji table from which to read.
   * @param overrides layout overrides to modify read behavior.
   * @return a new HBaseFijiTableReader.
   * @throws IOException in case of an error opening the reader.
   * @deprecated use {@link #createWithOptions}.
   */
  @Deprecated
  public static HBaseFijiTableReader createWithCellSpecOverrides(
      final HBaseFijiTable table,
      final Map<FijiColumnName, CellSpec> overrides
  ) throws IOException {
    return new HBaseFijiTableReader(table, overrides);
  }

  /**
   * Create a new <code>HBaseFijiTableReader</code> instance that sends read requests directly to
   * HBase.
   *
   * @param table Fiji table from which to read.
   * @param onDecoderCacheMiss behavior to use when a {@link ColumnReaderSpec} override
   *     specified in a {@link FijiDataRequest} cannot be found in the prebuilt cache of cell
   *     decoders.
   * @param overrides mapping from columns to overriding read behavior for those columns.
   * @param alternatives mapping from columns to reader spec alternatives which the
   *     FijiTableReader will accept as overrides in data requests.
   * @return a new HBaseFijiTableReader.
   * @throws IOException in case of an error opening the reader.
   */
  public static HBaseFijiTableReader createWithOptions(
      final HBaseFijiTable table,
      final OnDecoderCacheMiss onDecoderCacheMiss,
      final Map<FijiColumnName, ColumnReaderSpec> overrides,
      final Multimap<FijiColumnName, ColumnReaderSpec> alternatives
  ) throws IOException {
    return new HBaseFijiTableReader(table, onDecoderCacheMiss, overrides, alternatives);
  }

  /**
   * Open a table reader whose behavior is customized by overriding CellSpecs.
   *
   * @param table Fiji table from which this reader will read.
   * @param cellSpecOverrides specifications of overriding read behaviors.
   * @throws IOException in case of an error opening the reader.
   */
  private HBaseFijiTableReader(
      final HBaseFijiTable table,
      final Map<FijiColumnName, CellSpec> cellSpecOverrides
  ) throws IOException {
    mTable = table;
    mCellSpecOverrides = cellSpecOverrides;
    mOnDecoderCacheMiss = FijiTableReaderBuilder.DEFAULT_CACHE_MISS;
    mOverrides = null;
    mAlternatives = null;

    mLayoutConsumerRegistration = mTable.registerLayoutConsumer(new InnerLayoutUpdater());
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
   * Creates a new <code>HBaseFijiTableReader</code> instance that sends read requests directly to
   * HBase.
   *
   * @param table Fiji table from which to read.
   * @param onDecoderCacheMiss behavior to use when a {@link ColumnReaderSpec} override
   *     specified in a {@link FijiDataRequest} cannot be found in the prebuilt cache of cell
   *     decoders.
   * @param overrides mapping from columns to overriding read behavior for those columns.
   * @param alternatives mapping from columns to reader spec alternatives which the
   *     FijiTableReader will accept as overrides in data requests.
   * @throws IOException on I/O error.
   */
  private HBaseFijiTableReader(
      final HBaseFijiTable table,
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
          && !layoutColumns.contains(FijiColumnName.create(column.getFamily()))) {
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

    mLayoutConsumerRegistration = mTable.registerLayoutConsumer(new InnerLayoutUpdater());
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
  public FijiRowData get(EntityId entityId, FijiDataRequest dataRequest)
      throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get row from FijiTableReader instance %s in state %s.", this, state);

    final ReaderLayoutCapsule capsule = mReaderLayoutCapsule;
    // Make sure the request validates against the layout of the table.
    final FijiTableLayout tableLayout = capsule.getLayout();
    validateRequestAgainstLayout(dataRequest, tableLayout);

    // Construct an HBase Get to send to the HTable.
    HBaseDataRequestAdapter hbaseRequestAdapter =
        new HBaseDataRequestAdapter(dataRequest, capsule.getColumnNameTranslator());
    Get hbaseGet;
    try {
      hbaseGet = hbaseRequestAdapter.toGet(entityId, tableLayout);
    } catch (InvalidLayoutException e) {
      // The table layout should never be invalid at this point, since we got it from a valid
      // opened table.  If it is, there's something seriously wrong.
      throw new InternalFijiError(e);
    }
    // Send the HTable Get.
    final Result result = hbaseGet.hasFamilies() ? doHBaseGet(hbaseGet) : new Result();

    // Parse the result.
    return new HBaseFijiRowData(
        mTable, dataRequest, entityId, result, capsule.getCellDecoderProvider());
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
    final FijiTableLayout tableLayout = capsule.getLayout();
    validateRequestAgainstLayout(dataRequest, tableLayout);
    final HBaseDataRequestAdapter hbaseDataRequestAdapter =
        new HBaseDataRequestAdapter(dataRequest, capsule.getColumnNameTranslator());
    final Get get = hbaseDataRequestAdapter.toGet(entityId, tableLayout);
    final Result result = get.hasFamilies() ? doHBaseGet(get) : new Result();
    return HBaseFijiResult.create(
        entityId,
        dataRequest,
        result,
        mTable,
        capsule.getLayout(),
        capsule.getColumnNameTranslator(),
        capsule.getCellDecoderProvider());
  }

  /** {@inheritDoc} */
  @Override
  public List<FijiRowData> bulkGet(List<EntityId> entityIds, FijiDataRequest dataRequest)
      throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get rows from FijiTableReader instance %s in state %s.", this, state);

    // Bulk gets have some overhead associated with them,
    // so delegate work to get(EntityId, FijiDataRequest) if possible.
    if (entityIds.size() == 1) {
      return Collections.singletonList(this.get(entityIds.get(0), dataRequest));
    }
    final ReaderLayoutCapsule capsule = mReaderLayoutCapsule;
    final FijiTableLayout tableLayout = capsule.getLayout();
    validateRequestAgainstLayout(dataRequest, tableLayout);
    final HBaseDataRequestAdapter hbaseRequestAdapter =
        new HBaseDataRequestAdapter(dataRequest, capsule.getColumnNameTranslator());

    // Construct a list of hbase Gets to send to the HTable.
    final List<Get> hbaseGetList = makeGetList(entityIds, tableLayout, hbaseRequestAdapter);

    // Send the HTable Gets.
    final Result[] results = doHBaseGet(hbaseGetList);
    Preconditions.checkState(entityIds.size() == results.length);

    // Parse the results.  If a Result is null, then the corresponding FijiRowData should also
    // be null.  This indicates that there was an error retrieving this row.
    return parseResults(results, entityIds, dataRequest);
  }

  /** {@inheritDoc} */
  @Override
  public <T> List<FijiResult<T>> bulkGetResults(
      final List<EntityId> entityIds,
      final FijiDataRequest dataRequest
  ) throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get rows from FijiTableReader instance %s in state %s.", this, state);

    // Bulk gets have some overhead associated with them,
    // so delegate work to getResult(EntityId, FijiDataRequest) if possible.
    if (entityIds.size() == 1) {
      return Collections.singletonList(this.<T>getResult(entityIds.get(0), dataRequest));
    }
    final ReaderLayoutCapsule capsule = mReaderLayoutCapsule;
    final FijiTableLayout tableLayout = capsule.getLayout();
    validateRequestAgainstLayout(dataRequest, tableLayout);
    final HBaseDataRequestAdapter hbaseRequestAdapter =
        new HBaseDataRequestAdapter(dataRequest, capsule.getColumnNameTranslator());

    // Construct a list of hbase Gets to send to the HTable.
    final List<Get> hbaseGetList = makeGetList(entityIds, tableLayout, hbaseRequestAdapter);

    // Send the HTable Gets.
    final Result[] results = doHBaseGet(hbaseGetList);
    Preconditions.checkState(entityIds.size() == results.length);

    final List<FijiResult<T>> fijiResults = Lists.newArrayList();
    for (int i = 0; i < entityIds.size(); i++) {
      fijiResults.add(
          HBaseFijiResult.<T>create(
              entityIds.get(i),
              dataRequest,
              results[i],
              mTable,
              capsule.getLayout(),
              capsule.getColumnNameTranslator(),
              capsule.getCellDecoderProvider()
          )
      );
    }
    return fijiResults;
  }

  /** {@inheritDoc} */
  @Override
  public FijiRowScanner getScanner(FijiDataRequest dataRequest) throws IOException {
    return getScanner(dataRequest, new FijiScannerOptions());
  }

  /** {@inheritDoc} */
  @Override
  public FijiRowScanner getScanner(
      FijiDataRequest dataRequest,
      FijiScannerOptions fijiScannerOptions)
      throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get scanner from FijiTableReader instance %s in state %s.", this, state);

    try {
      EntityId startRow = fijiScannerOptions.getStartRow();
      EntityId stopRow = fijiScannerOptions.getStopRow();
      FijiRowFilter rowFilter = fijiScannerOptions.getFijiRowFilter();
      HBaseScanOptions scanOptions = fijiScannerOptions.getHBaseScanOptions();

      final ReaderLayoutCapsule capsule = mReaderLayoutCapsule;
      final HBaseDataRequestAdapter dataRequestAdapter =
          new HBaseDataRequestAdapter(dataRequest, capsule.getColumnNameTranslator());
      final FijiTableLayout tableLayout = capsule.getLayout();
      validateRequestAgainstLayout(dataRequest, tableLayout);
      final Scan scan = dataRequestAdapter.toScan(tableLayout, scanOptions);

      if (null != startRow) {
        scan.setStartRow(startRow.getHBaseRowKey());
      }
      if (null != stopRow) {
        scan.setStopRow(stopRow.getHBaseRowKey());
      }
      scan.setCaching(fijiScannerOptions.getRowCaching());

      if (null != rowFilter) {
        final FijiRowFilterApplicator applicator = FijiRowFilterApplicator.create(
            rowFilter, tableLayout, mTable.getFiji().getSchemaTable());
        applicator.applyTo(scan);
      }

      return new HBaseFijiRowScanner(new HBaseFijiRowScanner.Options()
          .withDataRequest(dataRequest)
          .withTable(mTable)
          .withScan(scan)
          .withCellDecoderProvider(capsule.getCellDecoderProvider())
          .withReopenScannerOnTimeout(fijiScannerOptions.getReopenScannerOnTimeout()));
    } catch (InvalidLayoutException e) {
      // The table layout should never be invalid at this point, since we got it from a valid
      // opened table.  If it is, there's something seriously wrong.
      throw new InternalFijiError(e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public <T> HBaseFijiResultScanner<T> getFijiResultScanner(
      final FijiDataRequest request
  ) throws IOException {
    return getFijiResultScanner(request, new FijiScannerOptions());
  }

  /**
   * Get a FijiResultScanner for the given data request and scan options.
   *
   * <p>
   *   This method allows the caller to specify a type-bound on the values of the {@code FijiCell}s
   *   of the returned {@code FijiResult}s. The caller should be careful to only specify an
   *   appropriate type. If the type is too specific (or wrong), a runtime
   *   {@link java.lang.ClassCastException} will be thrown when the returned {@code FijiResult} is
   *   used. See the 'Type Safety' section of {@code FijiResult}'s documentation for more details.
   * </p>
   *
   * @param request Data request defining the data to retrieve from each row.
   * @param scannerOptions Options to control the operation of the scanner.
   * @param <T> type {@code FijiCell} value returned by the {@code FijiResult}.
   * @return A new FijiResultScanner.
   * @throws IOException in case of an error creating the scanner.
   */
  public <T> HBaseFijiResultScanner<T> getFijiResultScanner(
      final FijiDataRequest request,
      final FijiScannerOptions scannerOptions
  ) throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get scanner from FijiTableReader instance %s in state %s.", this, state);

    final ReaderLayoutCapsule capsule = mReaderLayoutCapsule;
    final HBaseDataRequestAdapter adapter =
        new HBaseDataRequestAdapter(request, capsule.getColumnNameTranslator());
    final FijiTableLayout layout = capsule.getLayout();
    validateRequestAgainstLayout(request, layout);
    final Scan scan = adapter.toScan(layout, scannerOptions.getHBaseScanOptions());
    if (null != scannerOptions.getStartRow()) {
      scan.setStartRow(scannerOptions.getStartRow().getHBaseRowKey());
    }
    if (null != scannerOptions.getStopRow()) {
      scan.setStopRow(scannerOptions.getStopRow().getHBaseRowKey());
    }
    scan.setCaching(scannerOptions.getRowCaching());

    if (null != scannerOptions.getFijiRowFilter()) {
      final FijiRowFilterApplicator applicator = FijiRowFilterApplicator.create(
          scannerOptions.getFijiRowFilter(), layout, mTable.getFiji().getSchemaTable());
      applicator.applyTo(scan);
    }

    return new HBaseFijiResultScanner<T>(
        request,
        mTable,
        scan,
        capsule.getLayout(),
        capsule.getCellDecoderProvider(),
        capsule.getColumnNameTranslator(),
        scannerOptions.getReopenScannerOnTimeout());
  }

  /** {@inheritDoc} */
  @Override
  public <T> FijiResultScanner<T> getFijiResultScanner(
      final FijiDataRequest dataRequest,
      final FijiPartition partition
  ) throws IOException {
    Preconditions.checkArgument(partition instanceof HBaseFijiPartition,
        "Can not scan an HBase table with a non-HBase partition.");
    final HBaseFijiPartition hbasePartition = (HBaseFijiPartition) partition;

    final FijiScannerOptions options = new FijiScannerOptions();
    options.setStartRow(HBaseEntityId.fromHBaseRowKey(hbasePartition.getStartKey()));
    options.setStopRow(HBaseEntityId.fromHBaseRowKey(hbasePartition.getEndKey()));
    return getFijiResultScanner(dataRequest, options);
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(HBaseFijiTableReader.class)
        .add("id", System.identityHashCode(this))
        .add("table", mTable.getURI())
        .add("layout-version", mReaderLayoutCapsule.getLayout().getDesc().getLayoutId())
        .add("state", mState.get())
        .toString();
  }

  /**
   * Parses an array of hbase Results, returned from a bulk get, to a List of
   * FijiRowData.
   *
   * @param results The results to parse.
   * @param entityIds The matching set of EntityIds.
   * @param dataRequest The FijiDataRequest.
   * @return The list of FijiRowData returned by these results.
   * @throws IOException If there is an error.
   */
  private List<FijiRowData> parseResults(
      final Result[] results,
      final List<EntityId> entityIds,
      final FijiDataRequest dataRequest
  ) throws IOException {
    List<FijiRowData> rowDataList = new ArrayList<FijiRowData>(results.length);

    for (int i = 0; i < results.length; i++) {
      Result result = results[i];
      EntityId entityId = entityIds.get(i);

      final HBaseFijiRowData rowData = (null == result)
          ? null
          : new HBaseFijiRowData(mTable, dataRequest, entityId, result,
                mReaderLayoutCapsule.getCellDecoderProvider());
      rowDataList.add(rowData);
    }
    return rowDataList;
  }

  /**
   * Creates a list of hbase Gets for a set of entityIds.
   *
   * @param entityIds The set of entityIds to collect.
   * @param tableLayout The table layout specifying constraints on what data to return for a row.
   * @param hbaseRequestAdapter The HBaseDataRequestAdapter.
   * @return A list of hbase Gets-- one for each entity id.
   * @throws IOException If there is an error.
   */
  private static List<Get> makeGetList(List<EntityId> entityIds, FijiTableLayout tableLayout,
      HBaseDataRequestAdapter hbaseRequestAdapter)
      throws IOException {
    List<Get> hbaseGetList = new ArrayList<Get>(entityIds.size());
    try {
      for (EntityId entityId : entityIds) {
        hbaseGetList.add(hbaseRequestAdapter.toGet(entityId, tableLayout));
      }
      return hbaseGetList;
    } catch (InvalidLayoutException ile) {
      // The table layout should never be invalid at this point, since we got it from a valid
      // opened table.  If it is, there's something seriously wrong.
      throw new InternalFijiError(ile);
    }
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
    ResourceTracker.get().unregisterResource(this);
    mLayoutConsumerRegistration.close();
    mTable.release();
  }

  /**
   * Sends an HBase Get request.
   *
   * @param get HBase Get request.
   * @return the HBase Result.
   * @throws IOException on I/O error.
   */
  private Result doHBaseGet(Get get) throws IOException {
    final HTableInterface htable = mTable.openHTableConnection();
    try {
      LOG.debug("Sending HBase Get: {}", get);
      return htable.get(get);
    } finally {
      htable.close();
    }
  }

  /**
   * Sends a batch of HBase Get requests.
   *
   * @param get HBase Get requests.
   * @return the HBase Results.
   * @throws IOException on I/O error.
   */
  private Result[] doHBaseGet(List<Get> get) throws IOException {
    final HTableInterface htable = mTable.openHTableConnection();
    try {
      LOG.debug("Sending bulk HBase Get: {}", get);
      return htable.get(get);
    } finally {
      htable.close();
    }
  }
}

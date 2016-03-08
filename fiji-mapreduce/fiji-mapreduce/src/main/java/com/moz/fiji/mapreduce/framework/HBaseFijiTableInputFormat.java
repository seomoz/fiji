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

package com.moz.fiji.mapreduce.framework;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;
import com.moz.fiji.mapreduce.impl.FijiTableSplit;
import com.moz.fiji.schema.EntityId;
import com.moz.fiji.schema.HBaseEntityId;
import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiColumnName;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiRegion;
import com.moz.fiji.schema.FijiRowData;
import com.moz.fiji.schema.FijiRowScanner;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.FijiTableReader;
import com.moz.fiji.schema.FijiTableReader.FijiScannerOptions;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.filter.FijiRowFilter;
import com.moz.fiji.schema.hbase.HBaseScanOptions;
import com.moz.fiji.schema.impl.hbase.HBaseFijiRowData;
import com.moz.fiji.schema.impl.hbase.HBaseFijiTable;
import com.moz.fiji.schema.layout.ColumnReaderSpec;
import com.moz.fiji.schema.util.ResourceUtils;

/** InputFormat for Hadoop MapReduce jobs reading from a Fiji table. */
@ApiAudience.Framework
@ApiStability.Stable
public final class HBaseFijiTableInputFormat
    extends FijiTableInputFormat {

  /**
   * Number of bytes from the row-key to include when reporting progress.
   * Use 32 bits precision, ie. 4 billion row keys granularity.
   */
  private static final int PROGRESS_PRECISION_NBYTES = 4;

  /** Configuration of this input format. */
  private Configuration mConf;

  /** {@inheritDoc} */
  @Override
  public void setConf(Configuration conf) {
    mConf = conf;
  }

  /** {@inheritDoc} */
  @Override
  public Configuration getConf() {
    return mConf;
  }

  /** {@inheritDoc} */
  @Override
  public RecordReader<EntityId, FijiRowData> createRecordReader(
      InputSplit split,
      TaskAttemptContext context
  ) throws IOException {
    return new FijiTableRecordReader(mConf);
  }

  /**
   * Reports the HBase table name for the specified Fiji table.
   *
   * @param table Fiji table to report the HBase table name of.
   * @return the HBase table name for the specified Fiji table.
   * @throws java.io.IOException on I/O error.
   */
  private static byte[] getHBaseTableName(FijiTable table) throws IOException {
    final HBaseFijiTable htable = HBaseFijiTable.downcast(table);
    final HTableInterface hti = htable.openHTableConnection();
    try {
      return hti.getTableName();
    } finally {
      hti.close();
    }
  }

  /** {@inheritDoc} */
  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException {
    final Configuration conf = context.getConfiguration();
    final FijiURI inputTableURI =
        FijiURI.newBuilder(conf.get(FijiConfKeys.KIJI_INPUT_TABLE_URI)).build();
    final Fiji fiji = Fiji.Factory.open(inputTableURI, conf);
    try {
      final FijiTable table = fiji.openTable(inputTableURI.getTable());
      try {
        final byte[] htableName = getHBaseTableName(table);
        final List<InputSplit> splits = Lists.newArrayList();
        byte[] scanStartKey = HConstants.EMPTY_START_ROW;
        if (null != conf.get(FijiConfKeys.KIJI_START_ROW_KEY)) {
          scanStartKey = Base64.decodeBase64(conf.get(FijiConfKeys.KIJI_START_ROW_KEY));
        }
        byte[] scanLimitKey = HConstants.EMPTY_END_ROW;
        if (null != conf.get(FijiConfKeys.KIJI_LIMIT_ROW_KEY)) {
          scanLimitKey = Base64.decodeBase64(conf.get(FijiConfKeys.KIJI_LIMIT_ROW_KEY));
        }

        for (FijiRegion region : table.getRegions()) {
          final byte[] regionStartKey = region.getStartKey();
          final byte[] regionEndKey = region.getEndKey();
          // Determine if the scan start and limit key fall into the region.
          // Logic was copied from o.a.h.h.m.TableInputFormatBase
          if ((scanStartKey.length == 0 || regionEndKey.length == 0
               || Bytes.compareTo(scanStartKey, regionEndKey) < 0)
             && (scanLimitKey.length == 0
               || Bytes.compareTo(scanLimitKey, regionStartKey) > 0)) {
            byte[] splitStartKey = (scanStartKey.length == 0
              || Bytes.compareTo(regionStartKey, scanStartKey) >= 0)
              ? regionStartKey : scanStartKey;
            byte[] splitEndKey = ((scanLimitKey.length == 0
              || Bytes.compareTo(regionEndKey, scanLimitKey) <= 0)
              && regionEndKey.length > 0)
              ? regionEndKey : scanLimitKey;

            // TODO(KIJIMR-65): For now pick the first available location (ie. region server),
            // if any.
            final String location =
              region.getLocations().isEmpty() ? null : region.getLocations().iterator().next();
            final TableSplit tableSplit =
              new TableSplit(htableName, splitStartKey, splitEndKey, location);
            splits.add(new FijiTableSplit(tableSplit));
          }
        }
        return splits;

      } finally {
        ResourceUtils.releaseOrLog(table);
      }
    } finally {
      ResourceUtils.releaseOrLog(fiji);
    }
  }

  /** Hadoop record reader for Fiji table rows. */
  public static final class FijiTableRecordReader
      extends RecordReader<EntityId, FijiRowData> {

    private static final Logger LOG = LoggerFactory.getLogger(FijiTableRecordReader.class);

    /** Data request. */
    private final FijiDataRequest mDataRequest;

    private Fiji mFiji = null;
    private FijiTable mTable = null;
    private FijiTableReader mReader = null;
    private FijiRowScanner mScanner = null;
    private Iterator<FijiRowData> mIterator = null;
    private FijiTableSplit mSplit = null;
    private HBaseFijiRowData mCurrentRow = null;

    private long mStartPos;
    private long mStopPos;

    /**
     * Creates a new RecordReader for this input format.
     *
     * Perform the actual reads from Fiji.
     *
     * @param conf Configuration for the target Fiji.
     */
    private FijiTableRecordReader(Configuration conf) {
      // Get data request from the job configuration.
      final String dataRequestB64 = conf.get(FijiConfKeys.KIJI_INPUT_DATA_REQUEST);
      Preconditions.checkNotNull(dataRequestB64, "Missing data request in job configuration.");
      final byte[] dataRequestBytes = Base64.decodeBase64(Bytes.toBytes(dataRequestB64));
      mDataRequest = (FijiDataRequest) SerializationUtils.deserialize(dataRequestBytes);
    }

    /** {@inheritDoc} */
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
      Preconditions.checkArgument(split instanceof FijiTableSplit,
          "InputSplit is not a FijiTableSplit: %s", split);
      mSplit = (FijiTableSplit) split;

      final Configuration conf = context.getConfiguration();
      final FijiURI inputURI =
          FijiURI.newBuilder(conf.get(FijiConfKeys.KIJI_INPUT_TABLE_URI)).build();

      // When using Fiji tables as an input to MapReduce jobs, turn off block caching.
      final HBaseScanOptions hBaseScanOptions = new HBaseScanOptions();
      hBaseScanOptions.setCacheBlocks(false);

      // Extract the ColumnReaderSpecs and build a mapping from column to the appropriate overrides.
      final ImmutableMap.Builder<FijiColumnName, ColumnReaderSpec> overridesBuilder =
          ImmutableMap.builder();
      for (FijiDataRequest.Column column : mDataRequest.getColumns()) {
        if (column.getReaderSpec() != null) {
          overridesBuilder.put(column.getColumnName(), column.getReaderSpec());
        }
      }

      final FijiScannerOptions scannerOptions = new FijiScannerOptions()
          .setStartRow(HBaseEntityId.fromHBaseRowKey(mSplit.getStartRow()))
          .setStopRow(HBaseEntityId.fromHBaseRowKey(mSplit.getEndRow()))
          .setHBaseScanOptions(hBaseScanOptions);
      final String filterJson = conf.get(FijiConfKeys.KIJI_ROW_FILTER);
      if (null != filterJson) {
        final FijiRowFilter filter = FijiRowFilter.toFilter(filterJson);
        scannerOptions.setFijiRowFilter(filter);
      }
      mFiji = Fiji.Factory.open(inputURI, conf);
      mTable = mFiji.openTable(inputURI.getTable());
      mReader = mTable.getReaderFactory().readerBuilder()
          .withColumnReaderSpecOverrides(overridesBuilder.build())
          .buildAndOpen();
      mScanner = mReader.getScanner(mDataRequest, scannerOptions);
      mIterator = mScanner.iterator();
      mCurrentRow = null;

      mStartPos = bytesToPosition(mSplit.getStartRow(), PROGRESS_PRECISION_NBYTES);
      long stopPos = bytesToPosition(mSplit.getEndRow(), PROGRESS_PRECISION_NBYTES);
      mStopPos = (stopPos > 0) ? stopPos : (1L << (PROGRESS_PRECISION_NBYTES * 8));
      LOG.info("Progress reporting: start={} stop={}", mStartPos, mStopPos);
    }

    /** {@inheritDoc} */
    @Override
    public EntityId getCurrentKey() throws IOException {
      return mCurrentRow.getEntityId();
    }

    /** {@inheritDoc} */
    @Override
    public FijiRowData getCurrentValue() throws IOException {
      return mCurrentRow;
    }

    /**
     * Converts a byte array into an integer position in the row-key space.
     *
     * @param bytes Byte array to convert to an approximate position.
     * @param nbytes Number of bytes to use (must be in the range 1..8).
     * @return the approximate position in the row-key space.
     */
    public static long bytesToPosition(final byte[] bytes, final int nbytes) {
      long position = 0;
      if (bytes != null) {
        for (int i = 0; i < nbytes; ++i) {
          final int bvalue = (i < bytes.length) ? (0xff & bytes[i]) : 0;
          position = (position << 8) + bvalue;
        }
      }
      return position;
    }

    /**
     * Computes the start position from the start row key, for progress reporting.
     *
     * @param startRowKey Start row key to compute the position of.
     * @return the start position from the start row key.
     */
    public static long getStartPos(byte[] startRowKey) {
      return bytesToPosition(startRowKey, PROGRESS_PRECISION_NBYTES);
    }

    /**
     * Computes the stop position from the stop row key, for progress reporting.
     *
     * @param stopRowKey Stop row key to compute the position of.
     * @return the stop position from the start row key.
     */
    public static long getStopPos(byte[] stopRowKey) {
      long stopPos = bytesToPosition(stopRowKey, PROGRESS_PRECISION_NBYTES);
      return (stopPos > 0) ? stopPos : (1L << (PROGRESS_PRECISION_NBYTES * 8));
    }

    /**
     * Compute the progress (between 0.0f and 1.0f) for the current row key.
     *
     * @param startPos Computed start position (using getStartPos).
     * @param stopPos Computed stop position (using getStopPos).
     * @param currentRowKey Current row to compute a progress for.
     * @return the progress indicator for the given row, start and stop positions.
     */
    public static float computeProgress(long startPos, long stopPos, byte[] currentRowKey) {
      Preconditions.checkArgument(startPos <= stopPos,
          "Invalid start/stop positions: start=%s stop=%s", startPos, stopPos);
      final long currentPos = bytesToPosition(currentRowKey, PROGRESS_PRECISION_NBYTES);
      Preconditions.checkArgument(startPos <= currentPos,
          "Invalid start/current positions: start=%s current=%s", startPos, currentPos);
      Preconditions.checkArgument(currentPos <= stopPos,
          "Invalid current/stop positions: current=%s stop=%s", currentPos, stopPos);
      if (startPos == stopPos) {
        // Row key range is too small to perceive progress: report 50% completion
        return 0.5f;
      } else {
        return (float) (((double) currentPos - startPos) / (stopPos - startPos));
      }
    }

    /** {@inheritDoc} */
    @Override
    public float getProgress() throws IOException {
      if (mCurrentRow == null) {
        return 0.0f;
      }
      final byte[] currentRowKey = mCurrentRow.getHBaseResult().getRow();
      return computeProgress(mStartPos, mStopPos, currentRowKey);
    }

    /** {@inheritDoc} */
    @Override
    public boolean nextKeyValue() throws IOException {
      if (mIterator.hasNext()) {
        mCurrentRow = (HBaseFijiRowData) mIterator.next();
        return true;
      } else {
        mCurrentRow = null;
        return false;
      }
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException {
      ResourceUtils.closeOrLog(mScanner);
      ResourceUtils.closeOrLog(mReader);
      ResourceUtils.releaseOrLog(mTable);
      ResourceUtils.releaseOrLog(mFiji);

      mIterator = null;
      mScanner = null;
      mReader = null;
      mTable = null;
      mFiji = null;
      mSplit = null;
      mCurrentRow = null;
    }
  }
}

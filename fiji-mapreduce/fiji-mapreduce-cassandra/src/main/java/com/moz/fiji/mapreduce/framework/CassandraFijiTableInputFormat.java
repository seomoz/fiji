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

package com.moz.fiji.mapreduce.framework;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.Session;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;
import com.moz.fiji.schema.EntityId;
import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiColumnName;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiRowData;
import com.moz.fiji.schema.FijiRowScanner;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.FijiTableReader;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.cassandra.CassandraFijiURI;
import com.moz.fiji.schema.impl.cassandra.CassandraFijiScannerOptions;
import com.moz.fiji.schema.impl.cassandra.CassandraFijiTableReader;
import com.moz.fiji.schema.layout.ColumnReaderSpec;
import com.moz.fiji.schema.util.ResourceUtils;

/** InputFormat for Hadoop MapReduce jobs reading from a Fiji table. */
@ApiAudience.Framework
@ApiStability.Stable
public final class CassandraFijiTableInputFormat
    extends FijiTableInputFormat {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraFijiTableInputFormat.class);

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
    return new CassandraFijiTableRecordReader(mConf);
  }

  /** {@inheritDoc} */
  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException {
    final Configuration conf = context.getConfiguration();
    final CassandraFijiURI inputTableURI =
        CassandraFijiURI.newBuilder(conf.get(FijiConfKeys.FIJI_INPUT_TABLE_URI)).build();
    final Fiji fiji = Fiji.Factory.open(inputTableURI);
    try {
      final FijiTable table = fiji.openTable(inputTableURI.getTable());
      try {
        // Create a session with a custom load-balancing policy that will ensure that we send
        // queries for system.local and system.peers to the same node.
        final List<String> hosts = inputTableURI.getContactPoints();
        final String[] hostStrings = hosts.toArray(new String[hosts.size()]);
        int port = inputTableURI.getContactPort();
        final Cluster.Builder clusterBuilder = Cluster
            .builder()
            .addContactPoints(hostStrings)
            .withPort(port);

        if (null != inputTableURI.getUsername()) {
          clusterBuilder.withAuthProvider(
              new PlainTextAuthProvider(inputTableURI.getUsername(), inputTableURI.getPassword())
          );
        }

        final Cluster cluster = clusterBuilder
            .withLoadBalancingPolicy(new ConsistentHostOrderPolicy())
            .build();

        Session session = cluster.connect();

        // Get a list of all of the subsplits.  A "subsplit" contains the following:
        // - A token range (corresponding to a virtual node in the C* cluster)
        // - A list of replica nodes for that token range
        final CassandraSubSplitCreator cassandraSubSplitCreator =
            new CassandraSubSplitCreator(session);
        final List<CassandraSubSplit> subsplitsFromTokens =
            cassandraSubSplitCreator.createSubSplits();
        LOG.debug(String.format("Created %d subsplits from tokens", subsplitsFromTokens.size()));

        // In this InputFormat, we allow the user to specify a desired number of InputSplits.  We
        // will likely have far more subsplits (vnodes) than desired InputSplits.  Therefore, we
        // combine subsplits (hopefully those that share the same replica nodes) until we get to our
        // desired InputSplit count.
        final CassandraSubSplitCombiner cassandraSubSplitCombiner = new CassandraSubSplitCombiner();

        // Get a list of all of the token ranges in the Cassandra cluster.
        List<InputSplit> inputSplitList = Lists.newArrayList();

        // Get a target number of subsplits from the Hadoop configuration.
        final int targetNumberOfSubSplits = conf.getInt(
            CassandraFijiConfKeys.TARGET_NUMBER_OF_CASSANDRA_INPUT_SPLITS,
            CassandraSubSplitCombiner.DEFAULT_NUMBER_OF_SPLITS
        );

        // Java is annoying here about casting a list.
        inputSplitList.addAll(cassandraSubSplitCombiner.combineSubsplits(
            subsplitsFromTokens,
            targetNumberOfSubSplits));
        cluster.close();
        LOG.info(String.format("Created a total of %d InputSplits", inputSplitList.size()));
        for (InputSplit inputSplit : inputSplitList) {
          LOG.debug(inputSplit.toString());
        }
        return inputSplitList;
      } finally {
        ResourceUtils.releaseOrLog(table);
      }
    } finally {
      ResourceUtils.releaseOrLog(fiji);
    }
  }

  /** Hadoop record reader for Fiji table rows. */
  public static final class CassandraFijiTableRecordReader
      extends RecordReader<EntityId, FijiRowData> {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraFijiTableRecordReader.class);

    /** Data request. */
    private final FijiDataRequest mDataRequest;

    private Fiji mFiji = null;
    private FijiTable mTable = null;
    private CassandraFijiTableReader mReader = null;
    private FijiRowScanner mScanner = null;
    private Iterator<FijiRowData> mIterator = null;
    private CassandraInputSplit mSplit = null;
    private FijiRowData mCurrentRow = null;
    private Iterator<CassandraTokenRange> mTokenRangeIterator = null;

    private long mStartPos;
    private long mStopPos;

    /**
     * Creates a new RecordReader for this input format.
     *
     * Perform the actual reads from Fiji.
     *
     * @param conf Configuration for the target Fiji.
     */
    CassandraFijiTableRecordReader(Configuration conf) {
      // Get data request from the job configuration.
      final String dataRequestB64 = conf.get(FijiConfKeys.FIJI_INPUT_DATA_REQUEST);
      Preconditions.checkNotNull(dataRequestB64, "Missing data request in job configuration.");
      final byte[] dataRequestBytes = Base64.decodeBase64(Bytes.toBytes(dataRequestB64));
      mDataRequest = (FijiDataRequest) SerializationUtils.deserialize(dataRequestBytes);
    }

    /** {@inheritDoc} */
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
      initializeWithConf(split, context.getConfiguration());
    }

    /**
     * Version of `initialize` that takes a `Configuration.`  Makes testing easier.
     *
     * @param split Input split for this record reader.
     * @param conf Hadoop Configuration.
     * @throws java.io.IOException if there is a problem opening a connection to the Fiji instance.
     */
    void initializeWithConf(InputSplit split, Configuration conf) throws IOException {
      Preconditions.checkArgument(split instanceof CassandraInputSplit,
          "InputSplit is not a FijiTableSplit: %s", split);
      mSplit = (CassandraInputSplit) split;
      // Create an iterator to go through all of the token ranges.
      mTokenRangeIterator = mSplit.getTokenRangeIterator();
      assert(mTokenRangeIterator.hasNext());

      final FijiURI inputURI =
          FijiURI.newBuilder(conf.get(FijiConfKeys.FIJI_INPUT_TABLE_URI)).build();

      // TODO: Not sure if we need this...
      // Extract the ColumnReaderSpecs and build a mapping from column to the appropriate overrides.
      final ImmutableMap.Builder<FijiColumnName, ColumnReaderSpec> overridesBuilder =
          ImmutableMap.builder();
      for (FijiDataRequest.Column column : mDataRequest.getColumns()) {
        if (column.getReaderSpec() != null) {
          overridesBuilder.put(column.getColumnName(), column.getReaderSpec());
        }
      }

      mFiji = Fiji.Factory.open(inputURI);
      mTable = mFiji.openTable(inputURI.getTable());
      FijiTableReader reader = mTable.getReaderFactory().readerBuilder()
          .withColumnReaderSpecOverrides(overridesBuilder.build())
          .buildAndOpen();
      Preconditions.checkArgument(reader instanceof CassandraFijiTableReader);
      mReader = (CassandraFijiTableReader) reader;

      // TODO: Figure out progress from tokens.
      //mStartPos = bytesToPosition(mSplit.getStartRow(), PROGRESS_PRECISION_NBYTES);
      //long stopPos = bytesToPosition(mSplit.getEndRow(), PROGRESS_PRECISION_NBYTES);
      //mStopPos = (stopPos > 0) ? stopPos : (1L << (PROGRESS_PRECISION_NBYTES * 8));
      LOG.info("Progress reporting: start={} stop={}", mStartPos, mStopPos);
      queryNextTokenRange();
    }

    /**
     * Execute all of our queries over the next token range in our list of token ranges for this
     * input split.
     *
     * If were are out of token ranges, then set the current row and the current row iterator both
     * to null.
     *
     * @throws java.io.IOException if there is a problem building a scanner.
     */
    private void queryNextTokenRange() throws IOException {
      Preconditions.checkArgument(mTokenRangeIterator.hasNext());
      Preconditions.checkArgument(null == mIterator || !mIterator.hasNext());

      CassandraTokenRange nextTokenRange = mTokenRangeIterator.next();

      // Get a new scanner for this token range!
      if (null != mScanner) {
        ResourceUtils.closeOrLog(mScanner);
      }
      mScanner = mReader.getScannerWithOptions(
          mDataRequest,
          CassandraFijiScannerOptions.withTokens(
              nextTokenRange.getStartToken(),
              nextTokenRange.getEndToken()));
          mIterator = mScanner.iterator();
      mCurrentRow = null;
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
      /*
      if (mCurrentRow == null) {
        return 0.0f;
      }
      final byte[] currentRowKey = mCurrentRow.getHBaseResult().getRow();
      return computeProgress(mStartPos, mStopPos, currentRowKey);
      */
      return 0.0f;
    }

    /** {@inheritDoc} */
    @Override
    public boolean nextKeyValue() throws IOException {
      while (true) {
        if (mIterator.hasNext()) {
          mCurrentRow = mIterator.next();
          return true;
        }
        // We are out of rows in the current token range.
        Preconditions.checkArgument(!mIterator.hasNext());

        // We are also out of token ranges!
        if (!mTokenRangeIterator.hasNext()) {
          break;
        }

        // We still have more token ranges left!
        Preconditions.checkArgument(mTokenRangeIterator.hasNext());
        queryNextTokenRange();
      }

      mCurrentRow = null;
      return false;
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

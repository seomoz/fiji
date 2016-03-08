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

package com.moz.fiji.schema.mapreduce;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;
import com.moz.fiji.schema.EntityId;
import com.moz.fiji.schema.HBaseEntityId;
import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiRegion;
import com.moz.fiji.schema.FijiRowData;
import com.moz.fiji.schema.FijiRowScanner;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.FijiTableReader;
import com.moz.fiji.schema.FijiTableReader.FijiScannerOptions;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.FijiURIException;
import com.moz.fiji.schema.impl.hbase.HBaseFijiRowData;
import com.moz.fiji.schema.impl.hbase.HBaseFijiTable;
import com.moz.fiji.schema.util.ResourceUtils;

/** InputFormat for Hadoop MapReduce jobs reading from a Fiji table. */
@ApiAudience.Public
@ApiStability.Evolving
@Deprecated
public class FijiTableInputFormat
    extends InputFormat<EntityId, FijiRowData>
    implements Configurable {
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
      InputSplit split, TaskAttemptContext context) throws IOException {
    return new FijiTableRecordReader(mConf);
  }

  /** {@inheritDoc} */
  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException {
    final Configuration conf = context.getConfiguration();
    final FijiURI inputTableURI = getInputTableURI(conf);
    final Fiji fiji = Fiji.Factory.open(inputTableURI, conf);
    final FijiTable table = fiji.openTable(inputTableURI.getTable());

    final HTableInterface htable = HBaseFijiTable.downcast(table).openHTableConnection();
    try {
      final List<InputSplit> splits = Lists.newArrayList();
      for (FijiRegion region : table.getRegions()) {
        final byte[] startKey = region.getStartKey();
        // TODO: a smart way to get which location is most relevant.
        final String location =
            region.getLocations().isEmpty() ? null : region.getLocations().iterator().next();
        final TableSplit tableSplit = new TableSplit(
            htable.getTableName(), startKey, region.getEndKey(), location);
        splits.add(new FijiTableSplit(tableSplit, startKey));
      }
      return splits;

    } finally {
      htable.close();
    }
  }

  /**
   * Configures a Hadoop M/R job to read from a given table.
   *
   * @param job Job to configure.
   * @param tableURI URI of the table to read from.
   * @param dataRequest Data request.
   * @param startRow Minimum row key to process.
   * @param endRow Maximum row Key to process.
   * @throws IOException on I/O error.
   */
  public static void configureJob(
      Job job,
      FijiURI tableURI,
      FijiDataRequest dataRequest,
      String startRow,
      String endRow)
      throws IOException {

    final Configuration conf = job.getConfiguration();
    // As a precaution, be sure the table exists and can be opened.
    final Fiji fiji = Fiji.Factory.open(tableURI, conf);
    final FijiTable table = fiji.openTable(tableURI.getTable());
    ResourceUtils.releaseOrLog(table);
    ResourceUtils.releaseOrLog(fiji);

    // TODO: Check for jars config:
    // GenericTableMapReduceUtil.initTableInput(hbaseTableName, scan, job);

    // TODO: Obey specified start/end rows.

    // Write all the required values to the job's configuration object.
    job.setInputFormatClass(FijiTableInputFormat.class);
    final String serializedRequest =
        Base64.encodeBase64String(SerializationUtils.serialize(dataRequest));
    conf.set(FijiConfKeys.INPUT_DATA_REQUEST, serializedRequest);
    conf.set(FijiConfKeys.INPUT_TABLE_URI, tableURI.toString());
  }

  /** Hadoop record reader for Fiji table rows. */
  public static class FijiTableRecordReader
      extends RecordReader<EntityId, FijiRowData> {

    /** Data request. */
    protected final FijiDataRequest mDataRequest;

    /** Hadoop Configuration object containing settings. */
    protected final Configuration mConf;

    private Fiji mFiji = null;
    private FijiTable mTable = null;
    private FijiTableReader mReader = null;
    private FijiRowScanner mScanner = null;
    private Iterator<FijiRowData> mIterator = null;

    private FijiTableSplit mSplit = null;

    private HBaseFijiRowData mCurrentRow = null;

    /**
     * Creates a new RecordReader for this input format. This RecordReader will perform the actual
     * reads from Fiji.
     *
     * @param conf The configuration object for this Fiji.
     */
    public FijiTableRecordReader(Configuration conf) {
      mConf = conf;

      // Get data request from the job configuration.
      final String dataRequestB64 = checkNotNull(mConf.get(FijiConfKeys.INPUT_DATA_REQUEST),
          "Missing data request in job configuration.");
      final byte[] dataRequestBytes = Base64.decodeBase64(Bytes.toBytes(dataRequestB64));
      mDataRequest = (FijiDataRequest) SerializationUtils.deserialize(dataRequestBytes);
    }

    /** {@inheritDoc} */
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
      assert split instanceof FijiTableSplit;
      mSplit = (FijiTableSplit) split;

      final Configuration conf = context.getConfiguration();
      final FijiURI inputURI = getInputTableURI(conf);
      mFiji = Fiji.Factory.open(inputURI, conf);
      mTable = mFiji.openTable(inputURI.getTable());
      mReader = mTable.openTableReader();
      final FijiScannerOptions scannerOptions =
          new FijiScannerOptions()
          .setStartRow(HBaseEntityId.fromHBaseRowKey(mSplit.getStartRow()))
          .setStopRow(HBaseEntityId.fromHBaseRowKey(mSplit.getEndRow()));
      mScanner = mReader.getScanner(mDataRequest, scannerOptions);
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

    /** {@inheritDoc} */
    @Override
    public float getProgress() throws IOException {
      // TODO: Implement
      return 0.0f;
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

  /**
   * Reports the URI of the configured input table.
   *
   * @param conf Read the input URI from this configuration.
   * @return the configured input URI.
   * @throws IOException on I/O error.
   */
  private static FijiURI getInputTableURI(Configuration conf) throws IOException {
    try {
      return FijiURI.newBuilder(conf.get(FijiConfKeys.INPUT_TABLE_URI)).build();
    } catch (FijiURIException kue) {
      throw new IOException(kue);
    }
  }
}

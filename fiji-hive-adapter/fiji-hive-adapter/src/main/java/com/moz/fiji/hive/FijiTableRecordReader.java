/**
 * (c) Copyright 2013 WibiData, Inc.
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

package com.moz.fiji.hive;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.hive.io.FijiRowDataWritable;
import com.moz.fiji.hive.utils.FijiDataRequestSerializer;
import com.moz.fiji.schema.HBaseEntityId;
import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiDataRequestException;
import com.moz.fiji.schema.FijiRowData;
import com.moz.fiji.schema.FijiRowScanner;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.FijiTableReader;
import com.moz.fiji.schema.FijiTableReader.FijiScannerOptions;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.impl.hbase.HBaseFijiRowData;
import com.moz.fiji.schema.util.ResourceUtils;

/**
 * Reads key-value records from a FijiTableInputSplit (usually 1 region in an HTable).
 */
public class FijiTableRecordReader
    implements RecordReader<ImmutableBytesWritable, FijiRowDataWritable>, Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(FijiTableRecordReader.class);

  private final Fiji mFiji;
  private final FijiTable mFijiTable;
  private final FijiTableReader mFijiTableReader;
  private final FijiRowScanner mScanner;
  private final Iterator<FijiRowData> mIterator;

  private FijiRowDataWritable mCurrentPagedFijiRowDataWritable = null;

  /**
   * Constructor.
   *
   * @param inputSplit The input split to read records from.
   * @param conf The job configuration.
   * @throws IOException If the input split cannot be opened.
   */
  public FijiTableRecordReader(FijiTableInputSplit inputSplit, Configuration conf)
      throws IOException {
    FijiURI fijiURI = inputSplit.getFijiTableURI();
    mFiji = Fiji.Factory.open(fijiURI);
    mFijiTable = mFiji.openTable(fijiURI.getTable());
    mFijiTableReader = mFijiTable.openTableReader();

    final String hiveName = conf.get(FijiTableSerDe.HIVE_TABLE_NAME_PROPERTY);
    final String dataRequestParameter =
        FijiTableInputFormat.CONF_KIJI_DATA_REQUEST_PREFIX + hiveName;

    try {
      final String dataRequestString = conf.get(dataRequestParameter);
      if (null == dataRequestString) {
        throw new RuntimeException("FijiTableInputFormat was not configured. "
            + "Please set " + dataRequestParameter + " in configuration.");
      }
      FijiDataRequest dataRequest = FijiDataRequestSerializer.deserialize(dataRequestString);

      FijiScannerOptions scannerOptions = new FijiScannerOptions();
      if (inputSplit.getRegionStartKey().length > 0) {
        scannerOptions.setStartRow(HBaseEntityId.fromHBaseRowKey(inputSplit.getRegionStartKey()));
      }
      if (inputSplit.getRegionEndKey().length > 0) {
        scannerOptions.setStopRow(HBaseEntityId.fromHBaseRowKey(inputSplit.getRegionEndKey()));
      }
      mScanner = mFijiTableReader.getScanner(dataRequest, scannerOptions);
      mIterator = mScanner.iterator();
    } catch (FijiDataRequestException e) {
      throw new RuntimeException("Invalid FijiDataRequest.", e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    ResourceUtils.closeOrLog(mCurrentPagedFijiRowDataWritable);
    ResourceUtils.closeOrLog(mScanner);
    ResourceUtils.closeOrLog(mFijiTableReader);
    ResourceUtils.releaseOrLog(mFijiTable);
    ResourceUtils.releaseOrLog(mFiji);
  }

  /** {@inheritDoc} */
  @Override
  public ImmutableBytesWritable createKey() {
    return new ImmutableBytesWritable();
  }

  /** {@inheritDoc} */
  @Override
  public FijiRowDataWritable createValue() {
    return new FijiRowDataWritable();
  }

  /** {@inheritDoc} */
  @Override
  public long getPos() throws IOException {
    // There isn't a way to know how many bytes into the region we are.
    return 0L;
  }

  /** {@inheritDoc} */
  @Override
  public float getProgress() throws IOException {
    // TODO: Estimate progress based on the region row keys (if row key hashing is enabled).
    return 0.0f;
  }

  /** {@inheritDoc} */
  @Override
  public boolean next(ImmutableBytesWritable key, FijiRowDataWritable value) throws IOException {
    // If we're paging through a row, write it.  If it's empty, then move to the next row.
    if (mCurrentPagedFijiRowDataWritable != null
        && mCurrentPagedFijiRowDataWritable.hasMorePages()) {
      final FijiRowDataWritable.FijiRowDataPageWritable pagedResult =
          mCurrentPagedFijiRowDataWritable.nextPage();
      if (!pagedResult.isEmpty()) {
        key.set(mCurrentPagedFijiRowDataWritable.getEntityId().getHBaseRowKey());
        Writables.copyWritable(pagedResult, value);
        return true;
      }
    }

    // Close and null out current fiji row data writable before replacing it
    ResourceUtils.closeOrLog(mCurrentPagedFijiRowDataWritable);
    mCurrentPagedFijiRowDataWritable = null;

    // Stop if there are no more rows.
    if (!mIterator.hasNext()) {
      return false;
    }
    final HBaseFijiRowData rowData = (HBaseFijiRowData) mIterator.next();
    final FijiRowDataWritable result = new FijiRowDataWritable(rowData, mFijiTableReader);

    if (result.hasMorePages()) {
      // This is a paged row, so configure this reader to handle the paging, and write it if there
      // are any cells within it.  If there aren't, fall back and write the unpaged results.
      final FijiRowDataWritable.FijiRowDataPageWritable pagedResult = result.nextPage();
      if (!pagedResult.isEmpty()) {
        mCurrentPagedFijiRowDataWritable = result;
        key.set(mCurrentPagedFijiRowDataWritable.getEntityId().getHBaseRowKey());
        Writables.copyWritable(pagedResult, value);
        return true;
      }
    }

    key.set(rowData.getHBaseResult().getRow());
    Writables.copyWritable(result, value);
    ResourceUtils.closeOrLog(result);
    return true;
  }
}

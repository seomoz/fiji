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
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ScannerTimeoutException;
import org.apache.hadoop.hbase.regionserver.LeaseException;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.commons.ResourceTracker;
import com.moz.fiji.schema.EntityId;
import com.moz.fiji.schema.EntityIdFactory;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiIOException;
import com.moz.fiji.schema.FijiRowData;
import com.moz.fiji.schema.FijiRowScanner;
import com.moz.fiji.schema.layout.impl.CellDecoderProvider;

/**
 * The internal implementation of FijiRowScanner that reads from HTables.
 */
@ApiAudience.Private
public final class HBaseFijiRowScanner implements FijiRowScanner {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseFijiRowScanner.class);

  public static final String MAX_RETRIES_ON_TIMEOUT_PROPERTY =
      "com.moz.fiji.schema.impl.hbase.HBaseFijiRowScanner.MAX_RETRIES_ON_TIMEOUT";

  /** Maximum number of retries when (re-)opening the HBase scanner. */
  private static final int MAX_RETRIES_ON_TIMEOUT =
      Integer.parseInt(System.getProperty(MAX_RETRIES_ON_TIMEOUT_PROPERTY, "3"));

  /** The request used to fetch the row data. */
  private final FijiDataRequest mDataRequest;

  /** The table being scanned. */
  private final HBaseFijiTable mTable;

  /** HBase scan specification. */
  private final Scan mScan;

  /** Provider for cell decoders. */
  private final CellDecoderProvider mCellDecoderProvider;

  /** States of a row scanner instance. */
  private static enum State {
    UNINITIALIZED,
    OPEN,
    CLOSED
  }

  /** Tracks the state of this row scanner. */
  private final AtomicReference<State> mState = new AtomicReference<>(State.UNINITIALIZED);

  /** Factory for entity IDs. */
  private final EntityIdFactory mEntityIdFactory;

  /** Whether to reopen the HBase scanner on timeout. */
  private final boolean mReopenScannerOnTimeout;

  /** HTable connection. */
  private final HTableInterface mHTable;

  /** Current HBase result scanner. This scanner may timeout. */
  private ResultScanner mResultScanner = null;

  /** Result to return to the user on the following invocation of next(). */
  private Result mNextResult = null;

  /** HBase row key of the last result returned to the user. */
  private byte[] mLastReturnedKey = null;

  // -----------------------------------------------------------------------------------------------

  /**
   * A class to encapsulate the various options the HBaseFijiRowScanner constructor requires.
   */
  public static class Options {
    private FijiDataRequest mDataRequest;
    private HBaseFijiTable mTable;
    private Scan mScan;
    private CellDecoderProvider mCellDecoderProvider;
    private boolean mReopenScannerOnTimeout;

    /**
     * Sets the data request used to generate the FijiRowScanner.
     *
     * @param dataRequest A data request.
     * @return This options instance.
     */
    public Options withDataRequest(FijiDataRequest dataRequest) {
      mDataRequest = dataRequest;
      return this;
    }

    /**
     * Sets the table being scanned.
     *
     * @param table The table being scanned.
     * @return This options instance.
     */
    public Options withTable(HBaseFijiTable table) {
      mTable = table;
      return this;
    }

    /**
     * Sets the HBase scan specification.
     *
     * @param scan HBase scan specification.
     * @return This options instance.
     */
    public Options withScan(Scan scan) {
      mScan = scan;
      return this;
    }

    /**
     * Sets whether the HBase scanner should be reopened on timeout.
     *
     * @param reopenScannerOnTimeout Whether to reopen the HBase scanner on timeout.
     * @return This options instance.
     */
    public Options withReopenScannerOnTimeout(boolean reopenScannerOnTimeout) {
      mReopenScannerOnTimeout = reopenScannerOnTimeout;
      return this;
    }

    /**
     * Sets a provider for cell decoders.
     *
     * @param cellDecoderProvider Provider for cell decoders.
     * @return This options instance.
     */
    public Options withCellDecoderProvider(CellDecoderProvider cellDecoderProvider) {
      mCellDecoderProvider = cellDecoderProvider;
      return this;
    }

    /**
     * Gets the data request.
     *
     * @return The data request.
     */
    public FijiDataRequest getDataRequest() {
      return mDataRequest;
    }

    /**
     * Gets the table being scanned.
     *
     * @return The Fiji table.
     */
    public HBaseFijiTable getTable() {
      return mTable;
    }

    /**
     * Gets the HBase scan specification.
     *
     * @return the HBase scan specification.
     */
    public Scan getScan() {
      return mScan;
    }

    /**
     * Gets the provider for cell decoders.
     *
     * @return the provider for cell decoders.
     */
    public CellDecoderProvider getCellDecoderProvider() {
      return mCellDecoderProvider;
    }

    /**
     * Reports whether the HBase scanner should be re-opened on timeout.
     *
     * @return whether the HBase scanner should be re-opened on timeout.
     */
    public boolean getReopenScannerOnTimeout() {
      return mReopenScannerOnTimeout;
    }
  }

  // -----------------------------------------------------------------------------------------------

  /**
   * Creates a new <code>FijiRowScanner</code> instance.
   *
   * @param options The options for this scanner.
   * @throws IOException on I/O error.
   */
  public HBaseFijiRowScanner(Options options) throws IOException {
    mDataRequest = options.getDataRequest();
    mTable = options.getTable();
    mScan = options.getScan();
    mCellDecoderProvider = options.getCellDecoderProvider();
    mReopenScannerOnTimeout = options.getReopenScannerOnTimeout();

    mEntityIdFactory = EntityIdFactory.getFactory(mTable.getLayout());

    mHTable = mTable.openHTableConnection();
    try {
      mResultScanner = openResultScanner();
      mNextResult = getNextResult();
    } catch (FijiIOException ioe) {
      if (mHTable != null) {
        mHTable.close();
      }
      throw ioe;
    }

    final State oldState = mState.getAndSet(State.OPEN);
    Preconditions.checkState(oldState == State.UNINITIALIZED,
        "Cannot open FijiRowScanner instance in state %s.", oldState);
    ResourceTracker.get().registerResource(this);
  }

  /**
   * Computes the smallest possible HBase row key strictly greater than a given row key.
   *
   * @param rowKey A given HBase row key.
   * @return the smallest possible HBase row key strictly greater than the given row key.
   */
  private static byte[] getSmallestHigherThan(byte[] rowKey) {
    return Arrays.copyOf(rowKey, rowKey.length + 1);
  }

  /**
   * Opens a new HBase scanner.
   *
   * <p> Adjusts the start scanning row based on the last row returned to the user. </p>
   *
   * @return a new HBase scanner.
   */
  private ResultScanner openResultScanner() {
    try {
      if (mLastReturnedKey != null) {
        // If we previously returned a row to the user,
        // start the new scan at the lowest possible next row:
        mScan.setStartRow(getSmallestHigherThan(mLastReturnedKey));
      }
      LOG.debug("Opening HBase result scanner with start row key: '{}'.",
          Bytes.toStringBinary(mScan.getStartRow()));
      return mHTable.getScanner(mScan);
    } catch (IOException ioe) {
      throw new FijiIOException(ioe);
    }
  }

  /** {@inheritDoc} */
  @Override
  public FijiRowIterator iterator() {
    return new FijiRowIterator();
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    final State oldState = mState.getAndSet(State.CLOSED);
    Preconditions.checkState(oldState == State.OPEN,
        "Cannot close FijiRowScanner instance in state %s.", oldState);
    ResourceTracker.get().unregisterResource(this);
    mResultScanner.close();
    mHTable.close();
  }

  /**
   * Fetches the next HBase result.
   *
   * <p> Handles HBase scanner timeouts. </p>
   *
   * @return the next HBase result, or null if none.
   */
  private Result getNextResult() {
    for (int nretries = 0; nretries < MAX_RETRIES_ON_TIMEOUT; ++nretries) {
      try {
        return mResultScanner.next();

      } catch (LeaseException le) {
        if (!mReopenScannerOnTimeout) {
          LOG.debug("HBase scanner timed out and user disabled automatic scanner reopening.");
          throw new FijiIOException(
              "HBase scanner timed out and user disabled automatic scanner reopening.", le);
        } else {
          // The HBase scanner timed out, re-open a new one:
          LOG.debug("HBase scanner timed out: closing and reopening a new scanner.");
          mResultScanner.close();
          mResultScanner = openResultScanner();
          continue;
        }

      } catch (ScannerTimeoutException ste) {
        if (!mReopenScannerOnTimeout) {
          LOG.debug("HBase scanner timed out and user disabled automatic scanner reopening.");
          throw new FijiIOException(
              "HBase scanner timed out and user disabled automatic scanner reopening.", ste);
        } else {
          // The HBase scanner timed out, re-open a new one:
          LOG.debug("HBase scanner timed out: closing and reopening a new scanner.");
          mResultScanner.close();
          mResultScanner = openResultScanner();
          continue;
        }

      } catch (IOException ioe) {
        throw new FijiIOException(ioe);
      }
    }
    throw new FijiIOException("Unable to retrieve HBase result from scanner.");
  }

  // -----------------------------------------------------------------------------------------------

  /** Wraps a Fiji row scanner into a Java iterator. */
  private class FijiRowIterator implements Iterator<FijiRowData> {
    /** {@inheritDoc} */
    @Override
    public boolean hasNext() {
      final State state = mState.get();
      Preconditions.checkState(state == State.OPEN,
          "Cannot check has next on FijiRowScanner instance in state %s.", state);
      return (mNextResult != null);
    }

    /** {@inheritDoc} */
    @Override
    public FijiRowData next() {
      final State state = mState.get();
      Preconditions.checkState(state == State.OPEN,
          "Cannot get next on FijiRowScanner instance in state %s.", state);
      if (mNextResult == null) {
        // Comply with the Iterator interface:
        throw new NoSuchElementException();
      }
      final Result result = mNextResult;
      mLastReturnedKey = result.getRow();

      // Prefetch the next row for hasNext():
      mNextResult = getNextResult();

      // Decode the HBase result into a FijiRowData:
      try {
        final EntityId entityId = mEntityIdFactory.getEntityIdFromHBaseRowKey(result.getRow());
        return new HBaseFijiRowData(mTable, mDataRequest, entityId, result, mCellDecoderProvider);
      } catch (IOException ioe) {
        throw new FijiIOException(ioe);
      }
    }

    /** {@inheritDoc} */
    @Override
    public void remove() {
      throw new UnsupportedOperationException("FijiRowIterator does not support remove().");
    }
  }
}

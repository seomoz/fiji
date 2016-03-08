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

package com.moz.fiji.schema;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;
import com.moz.fiji.annotations.Inheritance;
import com.moz.fiji.schema.filter.FijiRowFilter;
import com.moz.fiji.schema.hbase.HBaseScanOptions;

/**
 * Interface for reading data from a Fiji table.
 *
 * <p>
 *   Utilizes {@link com.moz.fiji.schema.EntityId} and {@link com.moz.fiji.schema.FijiDataRequest}
 *   to return {@link com.moz.fiji.schema.FijiRowData} or a {@link com.moz.fiji.schema.FijiRowScanner}
 *   to iterate across rows in a Fiji table.
 * </p>
 *
 * <p>To get the three most recent versions of cell data from a column <code>bar</code> from
 * the family <code>foo</code> within the time range (123, 456):
 * <pre>{@code
 *   FijiDataRequestBuilder builder = FijiDataRequest.builder()
 *     .withTimeRange(123L, 456L);
 *     .newColumnsDef()
 *     .withMaxVersions(3)
 *     .add("foo", "bar");
 *   final FijiDataRequest request = builder.build();
 *
 *   final FijiTableReader reader = myFijiTable.openTableReader();
 *   final FijiRowData data = reader.get(myEntityId, request);
 * }</pre>
 * </p>
 *
 * <p>To get a row scanner across many records using the same column and version restrictions
 * from above:
 * <pre>{@code
 *   final FijiRowScanner scanner = reader.getScanner(request);
 *
 *   final FijiScannerOptions options = new FijiScannerOptions()
 *       .setStartRow(myStartRow)
 *       .setStopRow(myStopRow);
 *   final FijiRowScanner limitedScanner = reader.getScanner(request, options);
 * }</pre>
 *
 * If a FijiScannerOptions is not set, the scanner will iterate over all rows in the table
 * (as in the case of <code>scanner</code>).
 *
 * By default, Fiji row scanners automatically handle HBase scanner timeouts and reopen the
 * HBase scanner as needed. This behavior may be disabled using
 * {@link FijiScannerOptions#setReopenScannerOnTimeout(boolean)}.
 *
 * Finally, row caching may be configured via FijiScannerOptions.
 * By default, row caching is configured from the Hadoop Configuration property
 * {@code hbase.client.scanner.caching}.
 * </p>
 *
 * <p> Instantiated in Fiji Schema via {@link com.moz.fiji.schema.FijiTable#openTableReader()}. </p>
 * <p>
 *   Unless otherwise specified, readers are not thread-safe and must be synchronized externally.
 * </p>
 */
@ApiAudience.Public
@ApiStability.Evolving
@Inheritance.Sealed
public interface FijiTableReader extends Closeable {

  /**
   * Retrieves data from a single row in the fiji table.
   *
   * @param entityId The entity id for the row to get data from.
   * @param dataRequest Specifies the columns of data to retrieve.
   * @return The requested data. If there is no row for the specified entityId, this
   *     will return an empty FijiRowData. (containsColumn() will return false for all
   *     columns.)
   * @throws IOException If there is an IO error.
   */
  FijiRowData get(EntityId entityId, FijiDataRequest dataRequest)
      throws IOException;

  /**
   * Retrieves data from a sigle row in the fiji table.
   *
   * @param entityId The entity id for the row from which to get data.
   * @param dataRequest Specifies the columns of data to retrieve.
   * @param <T> Type of the data in the requested cells.
   * @return The requested data. Never null; if the requested row does not exist, returns an empty
   *     FijiResult.
   * @throws IOException If there is an error reading from the table.
   */
  <T> FijiResult<T> getResult(EntityId entityId, FijiDataRequest dataRequest)
      throws IOException;

  /**
   * Retrieves data from a list of rows in the fiji table.
   *
   * @param entityIds The list of entity ids to collect data for.
   * @param dataRequest Specifies constraints on the data to retrieve for each entity id.
   * @return The requested data.  If an EntityId specified in <code>entityIds</code>
   *     does not exist, then the corresponding FijiRowData will be empty.
   *     If a get fails, then the corresponding FijiRowData will be null (instead of empty).
   * @throws IOException If there is an IO error.
   */
  List<FijiRowData> bulkGet(List<EntityId> entityIds, FijiDataRequest dataRequest)
      throws IOException;

  /**
   * Retrieves data from a list of rows in a fiji table.
   *
   * @param entityIds List of entity ids from which to read data.
   * @param dataRequest Specifies the data to retrieve from each row.
   * @param <T> Type of the data to read.
   * @return The requested data. Elements are never null; if a requested row does not exist, the
   *     corresponding FijiResult will be empty.
   * @throws IOException In case of an error reading from the table.
   */
  <T> List<FijiResult<T>> bulkGetResults(List<EntityId> entityIds, FijiDataRequest dataRequest)
      throws IOException;

  /**
   * Gets a FijiRowScanner with the specified data request.
   *
   * @param dataRequest The data request to scan for.
   * @return The FijiRowScanner.
   * @throws IOException If there is an IO error.
   * @throws FijiDataRequestException If the data request is invalid.
   */
  FijiRowScanner getScanner(FijiDataRequest dataRequest)
      throws IOException;

  /**
   * Gets a FijiRowScanner using the specified data request and options.
   *
   * @param dataRequest The data request to scan for.
   * @param scannerOptions Other options for the scanner.
   * @return The FijiRowScanner.
   * @throws IOException If there is an IO error.
   * @throws FijiDataRequestException If the data request is invalid.
   */
  FijiRowScanner getScanner(FijiDataRequest dataRequest, FijiScannerOptions scannerOptions)
      throws IOException;

  /**
   * Get a FijiResultScanner using the specified data request. FijiScannerOptions are currently not
   * supported by FijiResultScanner.
   *
   * @param dataRequest Specifies the data to request from each row.
   * @param <T> Type of the data in the requested cells.
   * @return A FijiResultScanner for the requested data.
   * @throws IOException In case of an error reading from the table.
   */
  <T> FijiResultScanner<T> getFijiResultScanner(FijiDataRequest dataRequest)
      throws IOException;

  /**
   * Get a FijiResultScanner over the given partition using the specified data request.
   *
   * <p>
   *   The partition must belong to the same table as the scanner.
   * </p>
   *
   * @param dataRequest Specifies the data to request from each row.
   * @param partition The partition of the table to scan.
   * @param <T> Type of the data in the requested cells.
   * @return A FijiResultScanner for the requested data.
   * @throws IOException In case of an error reading from the table.
   */
  <T> FijiResultScanner<T> getFijiResultScanner(
      FijiDataRequest dataRequest,
      FijiPartition partition
  ) throws IOException;

  /**
   * Options for FijiRowScanners.
   */
  @ApiAudience.Public
  public static final class FijiScannerOptions {
    /** The start row for the scan. */
    private EntityId mStartRow = null;

    /** The stop row for the scan. */
    private EntityId mStopRow = null;

    /** The row filter for the scan. */
    private FijiRowFilter mRowFilter = null;

    /** When set, re-open scanners automatically on scanner timeout. */
    private boolean mReopenScannerOnTimeout = true;

    /**
     * Number of rows to fetch per RPC.
     * <ul>
     *   <li> N&gt;1 means fetch N rows per RPC; </li>
     *   <li> N=1 means no caching; </li>
     *   <li> N&lt;1 means use the value from the configuration property
     *        {@code hbase.client.scanner.caching}. </li>
     * </ul>
     */
    private int mRowCaching = -1;

    /**
     * The HBaseScanOptions to scan with for FijiRowScanners
     * backed by an HBase scan.
     *
     * Defaults to the default HBaseScanOptions if not set.
     */
    private HBaseScanOptions mHBaseScanOptions = new HBaseScanOptions();

    /**
     * Creates FijiScannerOptions with uninitialized options
     * and default HBaseScanOptions.
     */
    public FijiScannerOptions() {}

    /**
     * Sets the start row used by the scanner,
     * and returns this FijiScannerOptions to allow chaining.
     *
     * @param startRow The row to start scanning from.
     * @return This FijiScannerOptions with the start row set.
     */
    public FijiScannerOptions setStartRow(EntityId startRow) {
      mStartRow = startRow;
      return this;
    }

    /**
     * Gets the start row set in these options.
     *
     * @return The start row to use, null if unset.
     */
    public EntityId getStartRow() {
      return mStartRow;
    }

    /**
     * Sets the stop row used by the scanner,
     * and returns this FijiScannerOptions to allow chaining.
     *
     * @param stopRow The last row to scan.
     * @return This FijiScannerOptions with the stop row set.
     */
    public FijiScannerOptions setStopRow(EntityId stopRow) {
      mStopRow = stopRow;
      return this;
    }

    /**
     * Gets the stop row set in these options.
     *
     * @return The stop row to use, null if unset.
     */
    public EntityId getStopRow() {
      return mStopRow;
    }

    /**
     * Sets the row filter used by the scanner,
     * and returns this FijiScannerOptions to allow chaining.
     *
     * @param rowFilter The row filter to use.
     * @return This FijiScannerOptions with the row filter set.
     */
    public FijiScannerOptions setFijiRowFilter(FijiRowFilter rowFilter) {
      mRowFilter = rowFilter;
      return this;
    }

    /**
     * Gets the row filter set in these options.
     *
     * @return The row filter to use, null if unset.
     */
    public FijiRowFilter getFijiRowFilter() {
      return mRowFilter;
    }

    /**
     * Sets the HBaseScanOptions used by a HBase backed scanner.
     * The default is the default HBaseScanOptions.
     *
     * @param hBaseScanOptions The HBaseScanOptions to use.
     * @return This FijiScannerOptions with the HBaseScanOptions set.
     */
    public FijiScannerOptions setHBaseScanOptions(HBaseScanOptions hBaseScanOptions) {
      mHBaseScanOptions = hBaseScanOptions;
      return this;
    }

    /**
     * Gets the HBaseScanOptions set in these options.
     *
     * @return The HBaseScanOptions to use; if unset, the default HbaseScanOptions.
     */
    public HBaseScanOptions getHBaseScanOptions() {
      return mHBaseScanOptions;
    }

    /**
     * Configures whether the underlying HBase scanner should be automatically re-opened on timeout.
     *
     * <p> By default, timeouts are handled and the HBase scanner is automatically reopened. </p>
     *
     * @param reopenScannerOnTimeout True means HBase scanner timeouts are automatically
     *     handled to reopen new scanners. Otherwise ScannerTimeoutExceptions will be surfaced to
     *     the user.
     * @return this FijiScannerOptions.
     */
    public FijiScannerOptions setReopenScannerOnTimeout(boolean reopenScannerOnTimeout) {
      mReopenScannerOnTimeout = reopenScannerOnTimeout;
      return this;
    }

    /**
     * Reports whether the underlying HBase scanner should be reopened on timeout.
     *
     * @return whether the underlying HBase scanner should be reopened on timeout.
     */
    public boolean getReopenScannerOnTimeout() {
      return mReopenScannerOnTimeout;
    }

    /**
     * Configures the row caching, ie. number of rows to fetch per RPC to the region server.
     *
     * <ul>
     *   <li> N&gt;1 means fetch N rows per RPC; </li>
     *   <li> N=1 means no caching; </li>
     *   <li> N&lt;1 means use the value from the configuration property
     *        {@code hbase.client.scanner.caching}. </li>
     * </ul>
     * <p>
     *   By default, this is the value of the configuration property
     *   {@code hbase.client.scanner.caching}.
     * </p>
     *
     * @param rowCaching Number of rows to fetch per RPC to the region server.
     * @return this FijiScannerOptions.
     */
    public FijiScannerOptions setRowCaching(int rowCaching) {
      mRowCaching = rowCaching;
      return this;
    }

    /**
     * Reports the number of rows to fetch per RPC to the region server.
     *
     * <ul>
     *   <li> N&gt;1 means fetch N rows per RPC; </li>
     *   <li> N=1 means no caching; </li>
     *   <li> N&lt;1 means use the value from the configuration property
     *        {@code hbase.client.scanner.caching}. </li>
     * </ul>
     *
     * @return the number of rows to fetch per RPC to the region server.
     */
    public int getRowCaching() {
      return mRowCaching;
    }

  }
}

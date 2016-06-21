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

package com.moz.fiji.mapreduce.input;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;
import com.moz.fiji.mapreduce.MapReduceJobInput;
import com.moz.fiji.mapreduce.framework.FijiTableInputFormat;
import com.moz.fiji.mapreduce.tools.framework.JobIOConfKeys;
import com.moz.fiji.schema.EntityId;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.filter.FijiRowFilter;

/**
 * The class FijiTableMapReduceJobInput is used to indicate the usage of a FijiTable
 * as input to a MapReduce job. Any MapReduce job configured to read from a FijiTable
 * should expect to receive an {@link EntityId} as a key and a {@link com.moz.fiji.schema.FijiRowData}
 * as a value.
 *
 * <h2>Configuring an input:</h2>
 * <p>
 *   FijiTableMapReduceJobInput must be configured with a {@link FijiDataRequest}
 *   specifying the columns to read during the MapReduce job. FijiTableMapReduceJobInput
 *   can also be configured with optional row bounds that will limit section of rows
 *   that the job will use. Use a {@link RowOptions RowOptions} to specify these options:
 * </p>
 * <pre>
 *   <code>
 *     // Request the latest 3 versions of column 'info:email':
 *     final FijiDataRequestBuilder builder = FijiDataRequest.builder();
 *     builder.newColumnsDef().withMaxVersions(3).add("info", "email");
 *     final FijiDataRequest dataRequest = builder.build();
 *
 *     // Read from 'here' to 'there':
 *     final EntityId startRow = RawEntityId.getEntityId(Bytes.toBytes("here"));
 *     final EntityId limitRow = RawEntityId.getEntityId(Bytes.toBytes("there"));
 *     final FijiTableMapReduceJobInput.RowOptions rowOptions =
 *         new FijiTableMapReduceJobInput.RowOptions(startRow, limitRow, null);
 *     final MapReduceJobInput fijiTableJobInput = MapReduceJobInputs
 *         .newFijiTableMapReduceJobInput(mTable.getURI(), dataRequest, rowOptions);
 *   </code>
 * </pre>
 */
@ApiAudience.Public
@ApiStability.Stable
public final class FijiTableMapReduceJobInput extends MapReduceJobInput {
  /** URI of the input Fiji table. */
  private FijiURI mInputTableURI;

  /** Specifies which columns and versions of cells to read from the table. */
  private FijiDataRequest mDataRequest;

  /** Optional settings that specify which rows from the input table should be included. */
  private RowOptions mRowOptions;

  /**
   * Options that specify which rows from the input table should be included.
   *
   * <p>
   *   The settings here are used conjunctively with an AND operator.  In other words, a
   *   row will be included if and only if it is:
   * </p>
   *
   * <ul>
   *   <li>lexicographically equal to or after the start row, <em>and</em></li>
   *   <li>lexicographically before the limit row, <em>and</em></li>
   *   <li>accepted by the row filter.</li>
   * </ul>
   *
   * @see FijiRowFilter for more information about filtering which rows get read from
   *     the Fiji table.
   */
  public static final class RowOptions {
    /**
     * The start of the row range to read from the table (inclusive).  Use null to include
     * the first row in the table.
     */
    private final EntityId mStartRow;

    /**
     * The end of the row range to read from the table (exclusive).  Use null to include
     * the last row in the table.
     */
    private final EntityId mLimitRow;

    /**
     * A row filter that specifies whether a row from the table should be excluded.  Use
     * null to include all rows.
     */
    private final FijiRowFilter mRowFilter;

    /**
     * Creates a new <code>RowOptions</code> instance.
     *
     * @param startRow Entity id of the row to start reading from (inclusive). Specify null
     *     to indicate starting at the first row of the table.
     * @param limitRow Entity id of the row to stop reading at (exclusive). Specify null to
     *     indicate stopping after processing the last row of the table.
     * @param rowFilter A row filter to apply to the Fiji table. May be null.
     */
    private RowOptions(EntityId startRow, EntityId limitRow, FijiRowFilter rowFilter) {
      mStartRow = startRow;
      mLimitRow = limitRow;
      mRowFilter = rowFilter;
    }

    /**
     * Constructs options with default settings to include all the rows of the table.
     * @return a new RowOptions configured to include all rows of the table.
     */
    public static RowOptions create() {
      return new RowOptions(null, null, null);
    }

    /**
     * Creates a new <code>RowOptions</code> instance.
     *
     * @param startRow Entity id of the row to start reading from (inclusive). Specify null
     *     to indicate starting at the first row of the table.
     * @param limitRow Entity id of the row to stop reading at (exclusive). Specify null to
     *     indicate stopping after processing the last row of the table.
     * @param rowFilter A row filter to apply to the Fiji table. May be null.
     * @return a new RowOptions specifying on which rows of the table to operate.
     */
    public static RowOptions create(EntityId startRow, EntityId limitRow, FijiRowFilter rowFilter) {
      return new RowOptions(startRow, limitRow, rowFilter);
    }

    /**
     * Gets the entity id of the row to start reading from (inclusive). May be null to
     * indicate starting at the first row of the table.
     *
     * @return Entity id of the row to start reading from.
     */
    public EntityId getStartRow() {
      return mStartRow;
    }

    /**
     * Gets the entity id of the row to stop reading at (exclusive). May be null to
     * indicate ending after the last row of the table.
     *
     * @return Entity id of the row to stop reading at.
     */
    public EntityId getLimitRow() {
      return mLimitRow;
    }

    /** @return Row filter to apply to all rows being read (may be null). */
    public FijiRowFilter getRowFilter() {
      return mRowFilter;
    }
  }

  /**
   * Default constructor. Accessible via
   * {@link com.moz.fiji.mapreduce.input.MapReduceJobInputs#newFijiTableMapReduceJobInput()}.
   */
  FijiTableMapReduceJobInput() { }

  /** {@inheritDoc} */
  @Override
  public void initialize(Map<String, String> params) throws IOException {
    mInputTableURI = FijiURI.newBuilder(params.get(JobIOConfKeys.TABLE_KEY)).build();
  }

  /**
   * Constructs job input from column data in a Fiji table over a row range.  Accessible via
   * {@link MapReduceJobInputs#newFijiTableMapReduceJobInput(
   * com.moz.fiji.schema.FijiURI, com.moz.fiji.schema.FijiDataRequest,
   * com.moz.fiji.mapreduce.input.FijiTableMapReduceJobInput.RowOptions)}.
   *
   * @param inputTableURI URI of the input table.
   * @param dataRequest Specifies the columns and versions of cells to read from the table.
   * @param rowOptions Specifies optional settings for restricting the input from the
   *     table to some subset of the rows.
   */
  FijiTableMapReduceJobInput(
      FijiURI inputTableURI, FijiDataRequest dataRequest, RowOptions rowOptions) {
    mInputTableURI = inputTableURI;
    mDataRequest = dataRequest;
    mRowOptions = rowOptions;
  }

  /** {@inheritDoc} */
  @Override
  public void configure(Job job) throws IOException {
    // Configure the input format class.
    super.configure(job);
    FijiTableInputFormat inputFormat =
        FijiTableInputFormat.Factory.get(mInputTableURI).getInputFormat();
    job.setInputFormatClass(inputFormat.getClass());
    FijiTableInputFormat.configureJob(job, mInputTableURI, mDataRequest,
          null != mRowOptions ? mRowOptions.getStartRow() : null,
          null != mRowOptions ? mRowOptions.getLimitRow() : null,
          null != mRowOptions ? mRowOptions.getRowFilter() : null);
  }

  /** {@inheritDoc} */
  @Override
  protected Class<? extends InputFormat<?, ?>> getInputFormatClass() {
    FijiTableInputFormat inputFormat =
        FijiTableInputFormat.Factory.get(mInputTableURI).getInputFormat();
    return inputFormat.getClass();
  }

  /** @return Input table URI. */
  public FijiURI getInputTableURI() {
    return mInputTableURI;
  }

  /** @return Specified row options. */
  public RowOptions getRowOptions() {
    return mRowOptions;
  }
}

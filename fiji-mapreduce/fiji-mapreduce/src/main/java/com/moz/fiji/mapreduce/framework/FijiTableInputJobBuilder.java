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

import com.google.common.base.Preconditions;
import org.apache.hadoop.mapreduce.Job;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;
import com.moz.fiji.annotations.Inheritance;
import com.moz.fiji.mapreduce.JobConfigurationException;
import com.moz.fiji.mapreduce.MapReduceJobInput;
import com.moz.fiji.mapreduce.input.FijiTableMapReduceJobInput;
import com.moz.fiji.mapreduce.input.MapReduceJobInputs;
import com.moz.fiji.schema.EntityId;
import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiDataRequestException;
import com.moz.fiji.schema.FijiDataRequestValidator;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.filter.FijiRowFilter;
import com.moz.fiji.schema.util.ResourceUtils;

/**
 * Base class for MapReduce jobs that use a Fiji table as input.
 *
 * @param <T> Type of the builder class.
 */
@ApiAudience.Framework
@ApiStability.Stable
@Inheritance.Sealed
public abstract class FijiTableInputJobBuilder<T extends FijiTableInputJobBuilder<T>>
    extends MapReduceJobBuilder<T> {
  /** The table to use as input for the job. */
  private FijiURI mInputTableURI;

  /** The entity id of the start row (inclusive). */
  private EntityId mStartRow;

  /** The entity id of the limit row (exclusive). */
  private EntityId mLimitRow;

  /** A row filter that specifies rows to exclude from the scan (optional, so may be null). */
  private FijiRowFilter mRowFilter;

  /** Constructs a builder for jobs that use a Fiji table as input. */
  protected FijiTableInputJobBuilder() {
    mInputTableURI = null;
    mStartRow = null;
    mLimitRow = null;
    mRowFilter = null;
  }

  /**
   * Configures the job input table.
   *
   * @param input The job input table.
   * @return This builder instance so you may chain configuration method calls.
   */
  @SuppressWarnings("unchecked")
  public final T withJobInput(FijiTableMapReduceJobInput input) {
    mInputTableURI = input.getInputTableURI();
    if (input.getRowOptions() != null) {
      mStartRow = input.getRowOptions().getStartRow();
      mLimitRow = input.getRowOptions().getLimitRow();
      mRowFilter = input.getRowOptions().getRowFilter();
    }
    return (T) this;
  }

  /**
   * Configures the job with the Fiji table to use as input.
   *
   * @param inputTableURI The Fiji table to use as input for the job.
   * @return This builder instance so you may chain configuration method calls.
   */
  @SuppressWarnings("unchecked")
  public final T withInputTable(FijiURI inputTableURI) {
    mInputTableURI = inputTableURI;
    return (T) this;
  }

  /**
   * Configures the job to process rows after and including an entity id.
   *
   * @param entityId The entity id of the first row input.
   * @return This builder instance so you may chain configuration method calls.
   */
  @SuppressWarnings("unchecked")
  public final T withStartRow(EntityId entityId) {
    mStartRow = entityId;
    return (T) this;
  }

  /**
   * Configures the job to process rows before an entity id.
   *
   * @param entityId The entity id of the first row to exclude from the input.
   * @return This builder instance so you may chain configuration method calls.
   */
  @SuppressWarnings("unchecked")
  public final T withLimitRow(EntityId entityId) {
    mLimitRow = entityId;
    return (T) this;
  }

  /**
   * Configures the job to exclude rows not accepted by a row filter.
   *
   * @param rowFilter A filter that specifies which rows to exclude from the input table.
   * @return This builder instance so you may chain configuration method calls.
   */
  @SuppressWarnings("unchecked")
  public final T withFilter(FijiRowFilter rowFilter) {
    mRowFilter = rowFilter;
    return (T) this;
  }

  /** {@inheritDoc} */
  @Override
  protected final MapReduceJobInput getJobInput() {
    final FijiTableMapReduceJobInput.RowOptions rowOptions =
        FijiTableMapReduceJobInput.RowOptions.create(mStartRow, mLimitRow, mRowFilter);
    return MapReduceJobInputs.newFijiTableMapReduceJobInput(
        mInputTableURI, getDataRequest(), rowOptions);
  }

  /** @return the URI of the input table. */
  protected final FijiURI getInputTableURI() {
    return mInputTableURI;
  }

  /**
   * Subclasses must override this to provide a Fiji data request for the input table.
   *
   * @return the Fiji data request to configure the input table scanner with.
   */
  protected abstract FijiDataRequest getDataRequest();

  /** {@inheritDoc} */
  @Override
  protected void configureJob(Job job) throws IOException {
    // Configure the input, mapper, combiner, and reducer, output.
    super.configureJob(job);

    // Validate the Fiji data request against the current table layout:
    Preconditions.checkNotNull(mInputTableURI, "Input Fiji table was never set.");
    final Fiji fiji = Fiji.Factory.open(mInputTableURI, getConf());
    try {
      final FijiTable table = fiji.openTable(mInputTableURI.getTable());
      try {
        validateInputTable(table);
      } finally {
        ResourceUtils.releaseOrLog(table);
      }
    } finally {
      ResourceUtils.releaseOrLog(fiji);
    }
  }

  /**
   * Validates the input table.
   *
   * Sub-classes may override this method to perform additional validation requiring an active
   * connection to the input table.
   *
   * @param table Input table.
   * @throws IOException on I/O error.
   */
  protected void validateInputTable(FijiTable table) throws IOException {
    try {
      FijiDataRequestValidator.validatorForLayout(table.getLayout()).validate(getDataRequest());
    } catch (FijiDataRequestException kdre) {
      throw new JobConfigurationException("Invalid data request: " + kdre.getMessage());
    }
  }
}

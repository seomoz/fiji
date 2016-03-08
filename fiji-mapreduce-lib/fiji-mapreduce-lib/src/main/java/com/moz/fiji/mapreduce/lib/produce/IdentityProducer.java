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

package com.moz.fiji.mapreduce.lib.produce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import com.moz.fiji.hadoop.configurator.HadoopConf;
import com.moz.fiji.hadoop.configurator.HadoopConfigurator;
import com.moz.fiji.mapreduce.produce.FijiProducer;
import com.moz.fiji.mapreduce.produce.ProducerContext;
import com.moz.fiji.schema.FijiColumnName;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiDataRequestBuilder;
import com.moz.fiji.schema.FijiRowData;

/**
 * This producer copies data from one family or column to another without modification.
 *
 * <p>To use this producer, you must specify an <i>input</i> and an <i>output</i>.  The
 * input may be a single column of the form <i>"family:qualifier"</i>, or an entire family
 * of the form <i>"family"</i>.  The input will be copied to the target output column or
 * family.</p>
 *
 * <p>To specify the input column name, set the configuration variable
 * <i>identity.producer.input</i>.  The output column name is set with the configuration
 * variable <i>identity.producer.output</i>.</p>
 */
public class IdentityProducer extends FijiProducer {
  public static final String CONF_INPUT = "identity.producer.input";
  public static final String CONF_OUTPUT = "identity.producer.output";

  private FijiColumnName mInputColumn;
  private FijiColumnName mOutputColumn;

  /** {@inheritDoc} */
  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    HadoopConfigurator.configure(this);

    // Validate that they are either both families or both columns.
    if (mInputColumn.isFullyQualified() != mOutputColumn.isFullyQualified()) {
      throw new RuntimeException(
          "Input and output must both be a specific column, or both be a family");
    }
  }

  /**
   * Sets the input column name.
   *
   * @param column The input column.
   */
  @HadoopConf(key=CONF_INPUT, usage="The input column name.")
  protected void setInputColumn(String column) {
    if (null == column || column.isEmpty()) {
      throw new RuntimeException("Must specify " + CONF_INPUT);
    }
    mInputColumn = new FijiColumnName(column);
  }

  /**
   * Sets the output column name.
   *
   * @param column The output column.
   */
  @HadoopConf(key=CONF_OUTPUT, usage="The output column name.")
  protected void setOutputColumn(String column) {
    if (null == column || column.isEmpty()) {
      throw new RuntimeException("Must specify " + CONF_OUTPUT);
    }
    mOutputColumn = new FijiColumnName(column);
  }

  /** {@inheritDoc} */
  @Override
  public FijiDataRequest getDataRequest() {
    FijiDataRequestBuilder builder = FijiDataRequest.builder();
    builder.newColumnsDef().withMaxVersions(Integer.MAX_VALUE)
        .add(mInputColumn.getFamily(), mInputColumn.getQualifier());
    return builder.build();
  }

  /** {@inheritDoc} */
  @Override
  public String getOutputColumn() {
    return mOutputColumn.toString();
  }

  /** {@inheritDoc} */
  @Override
  public void produce(FijiRowData input, ProducerContext context)
      throws IOException {

    if (!mInputColumn.isFullyQualified()) {
      // Copy the entire family.
      for (String qualifier : input.getQualifiers(mInputColumn.getFamily())) {
        FijiColumnName sourceColumn = new FijiColumnName(mInputColumn.getFamily(), qualifier);
        produceAllVersions(input, context, sourceColumn);
      }
    } else {
      // Copy just a specific column.
      produceAllVersions(input, context, mInputColumn);
    }
  }

  /**
   * Produces all data from a given column name into the output column.
   *
   * @param input The input row.
   * @param context The producer context used to write.
   * @param columnName The column to read from.
   * @throws IOException If there is an IO error.
   */
  private void produceAllVersions(
      FijiRowData input, ProducerContext context, FijiColumnName columnName)
      throws IOException {
    for (long timestamp : input.getTimestamps(columnName.getFamily(), columnName.getQualifier())) {
      // Read the data from the input column.
      Object data = input.getValue(
          mInputColumn.getFamily(), columnName.getQualifier(), timestamp);

      // Write the data to the output column.
      if (!mOutputColumn.isFullyQualified()) {
        context.put(columnName.getQualifier(), timestamp, data);
      } else {
        context.put(timestamp, data);
      }
    }
  }
}

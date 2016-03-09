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

package com.moz.fiji.mapreduce.produce.impl;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.mapreduce.framework.FijiConfKeys;
import com.moz.fiji.mapreduce.produce.FijiProducer;
import com.moz.fiji.mapreduce.produce.FijiProducerOutputException;
import com.moz.fiji.schema.FijiColumnName;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.layout.FijiTableLayout.LocalityGroupLayout.FamilyLayout;

/** Utility methods for working with <code>FijiProducer</code>s. */
@ApiAudience.Private
public final class FijiProducers {
  /** Disable the constructor for this utility class. */
  private FijiProducers() {}

  /**
   * Creates an instance of the producer specified by the
   * {@link org.apache.hadoop.conf.Configuration}.
   *
   * <p>The configuration would have stored the producer name only if
   * it was configured by a FijiProduceJobBuilder, so don't try calling this
   * method with any old Configuration object.</p>
   *
   * @param conf The job configuration.
   * @return a brand-spankin'-new FijiProducer instance.
   * @throws IOException If the producer name cannot be instantiated from the configuration.
   */
  public static FijiProducer create(Configuration conf) throws IOException {
    final Class<? extends FijiProducer> producerClass =
        conf.getClass(FijiConfKeys.FIJI_PRODUCER_CLASS, null, FijiProducer.class);
    if (null == producerClass) {
      throw new IOException("Producer class could not be found in configuration.");
    }
    return ReflectionUtils.newInstance(producerClass, conf);
  }

  /**
   * Loads a FijiProducer class by name.
   *
   * @param className Fully qualified name of the class to load.
   * @return the loaded class.
   * @throws ClassNotFoundException if the class is not found.
   * @throws ClassCastException if the class is not a FijiGatherer.
   */
  public static Class<? extends FijiProducer> forName(String className)
      throws ClassNotFoundException {
    return Class.forName(className).asSubclass(FijiProducer.class);
  }

  /**
   * Makes sure the producer's requested output column exists in the
   * fiji table layout.
   *
   * @param producer The producer whose output column should be validated.
   * @param tableLayout The layout of the table to validate the output column against.
   * @throws FijiProducerOutputException If the output column cannot be written to.
   */
  public static void validateOutputColumn(FijiProducer producer, FijiTableLayout tableLayout)
      throws FijiProducerOutputException {
    final String outputColumn = producer.getOutputColumn();
    if (null == outputColumn) {
      throw new FijiProducerOutputException(String.format(
          "Producer '%s' must specify an output column by overridding getOutputColumn().",
          producer.getClass().getName()));
    }

    final FijiColumnName columnName = new FijiColumnName(outputColumn);
    final FamilyLayout family = tableLayout.getFamilyMap().get(columnName.getFamily());
    if (null == family) {
      throw new FijiProducerOutputException(String.format(
          "Producer '%s' specifies unknown output column family '%s' in table '%s'.",
          producer.getClass().getName(), columnName.getFamily(), tableLayout.getName()));
    }

    // When writing to a particular column qualifier of a group, make sure it exists:
    if (columnName.isFullyQualified()
        && family.isGroupType()
        && !family.getColumnMap().containsKey(columnName.getQualifier())) {
      throw new FijiProducerOutputException(String.format(
          "Producer '%s' specifies unknown column '%s' in table '%s'.",
          producer.getClass().getName(), columnName, tableLayout.getName()));
    }
  }
}

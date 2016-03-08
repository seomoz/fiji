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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.mapreduce.FijiContext;
import com.moz.fiji.mapreduce.produce.FijiProducer;
import com.moz.fiji.schema.FijiColumnName;
import com.moz.fiji.schema.FijiDataRequest;

/**
 * Base class for producers that read from the most recent value of a single input column.
 */
public abstract class SingleInputProducer extends FijiProducer {
  private static final Logger LOG = LoggerFactory.getLogger(SingleInputProducer.class);

  private FijiColumnName mInputColumn;

  /**
   * @return the name of the fiji input column to feed to this producer.
   */
  protected abstract String getInputColumn();

  /**
   * Initialize the family and qualifier instance variables.
   */
  private void initializeInputColumn() {
    mInputColumn = new FijiColumnName(getInputColumn());
    if (!mInputColumn.isFullyQualified()) {
      throw new RuntimeException("getInputColumn() must contain a colon (':')");
    }
  }

  /** {@inheritDoc} */
  @Override
  public FijiDataRequest getDataRequest() {
    initializeInputColumn();
    return FijiDataRequest.create(mInputColumn.getFamily(), mInputColumn.getQualifier());
  }

  /** {@inheritDoc} */
  @Override
  public void setup(FijiContext context) throws IOException {
    initializeInputColumn();
  }

  /** @return the input column family. */
  protected FijiColumnName getInputColumnName() {
    return mInputColumn;
  }
}

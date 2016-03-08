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

package com.moz.fiji.schema.impl.hbase;

import java.io.IOException;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.schema.AtomicFijiPutter;
import com.moz.fiji.schema.FijiBufferedWriter;
import com.moz.fiji.schema.FijiTableWriter;
import com.moz.fiji.schema.FijiWriterFactory;

/** Factory for Table Writers. */
@ApiAudience.Private
public final class HBaseFijiWriterFactory implements FijiWriterFactory {

  /** HBaseFijiTable for this writer factory. */
  private final HBaseFijiTable mTable;

  /**
   * Constructor for this writer factory.
   *
   * @param table The HBaseFijiTable to which this writer factory's writers write.
   */
  public HBaseFijiWriterFactory(HBaseFijiTable table) {
    mTable = table;
  }

  /** {@inheritDoc} */
  @Override
  public FijiTableWriter openTableWriter() throws IOException {
    return new HBaseFijiTableWriter(mTable);
  }

  /** {@inheritDoc} */
  @Override
  public AtomicFijiPutter openAtomicPutter() throws IOException {
    return new HBaseAtomicFijiPutter(mTable);
  }

  /** {@inheritDoc} */
  @Override
  public FijiBufferedWriter openBufferedWriter() throws IOException {
    return new HBaseFijiBufferedWriter(mTable);
  }
}

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
import java.util.Map;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.schema.FijiColumnName;
import com.moz.fiji.schema.FijiReaderFactory;
import com.moz.fiji.schema.layout.CellSpec;

/** Factory for Table Writers. */
@ApiAudience.Private
public final class HBaseFijiReaderFactory implements FijiReaderFactory {

  /** HBaseFijiTable for this writer factory. */
  private final HBaseFijiTable mTable;

  /**
   * Initializes a factory for HBaseFijiTable readers.
   *
   * @param table HBaseFijiTable for which to construct readers.
   */
  public HBaseFijiReaderFactory(HBaseFijiTable table) {
    mTable = table;
  }

  /** {@inheritDoc} */
  @Override
  public HBaseFijiTable getTable() {
    return mTable;
  }

  /** {@inheritDoc} */
  @Override
  public HBaseFijiTableReader openTableReader() throws IOException {
    return HBaseFijiTableReader.create(mTable);
  }

  /** {@inheritDoc} */
  @Override
  public HBaseFijiTableReader openTableReader(Map<FijiColumnName, CellSpec> overrides)
      throws IOException {
    return HBaseFijiTableReader.createWithCellSpecOverrides(mTable, overrides);
  }

  /** {@inheritDoc} */
  @Override
  public HBaseFijiTableReaderBuilder readerBuilder() {
    return HBaseFijiTableReaderBuilder.create(mTable);
  }

}

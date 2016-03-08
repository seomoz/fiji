/**
 * (c) Copyright 2014 WibiData, Inc.
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

import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiResultScanner;
import com.moz.fiji.schema.FijiRowScanner;
import com.moz.fiji.schema.FijiRowScannerTest;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.FijiTableReader;
import com.moz.fiji.schema.FijiTableReader.FijiScannerOptions;
import com.moz.fiji.schema.impl.FijiResultRowScanner;

/**
 * A test of {@link com.moz.fiji.schema.impl.FijiResultRowScanner} over an
 * {@link com.moz.fiji.schema.impl.hbase.HBaseFijiResultScanner}.
 */
public class TestHBaseFijiResultRowScanner extends FijiRowScannerTest {

  /** {@inheritDoc} */
  @Override
  public FijiRowScanner getRowScanner(
      final FijiTable table,
      final FijiTableReader reader,
      final FijiDataRequest dataRequest
  ) throws IOException {
    // Todo(SCHEMA-868): drop cast
    final HBaseFijiTableReader hbaseReader = (HBaseFijiTableReader) reader;
    final FijiResultScanner<Object> resultScanner = hbaseReader.getFijiResultScanner(
        dataRequest,
        new FijiScannerOptions());
    return new FijiResultRowScanner(table.getLayout(), resultScanner);
  }
}

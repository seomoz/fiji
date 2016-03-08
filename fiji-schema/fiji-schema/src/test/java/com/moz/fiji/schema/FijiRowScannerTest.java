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

package com.moz.fiji.schema;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Iterator;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.moz.fiji.schema.FijiTableReader.FijiScannerOptions;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.layout.FijiTableLayouts;
import com.moz.fiji.schema.util.InstanceBuilder;
import com.moz.fiji.schema.util.ResourceUtils;

/**
 * Test of {@link com.moz.fiji.schema.FijiRowData} for HBase Fiji scans.
 */
public abstract class FijiRowScannerTest extends FijiClientTest {

  private Fiji mFiji;
  private FijiTable mTable;
  private FijiTableReader mReader;

  public abstract FijiRowScanner getRowScanner(
      final FijiTable table,
      final FijiTableReader reader,
      final FijiDataRequest dataRequest
  ) throws IOException;

  @Before
  public final void setupEnvironment() throws Exception {
    // Get the test table layouts.
    final FijiTableLayout layout = FijiTableLayout.newLayout(
        FijiTableLayouts.getLayout(FijiTableLayouts.COUNTER_TEST));

    // Populate the environment.
    mFiji = new InstanceBuilder(getFiji())
        .withTable("user", layout)
        .withRow("foo")
        .withFamily("info")
        .withQualifier("name").withValue(1L, "foo-val")
        .withRow("bar")
        .withFamily("info")
        .withQualifier("name").withValue(1L, "bar-val")
        .build();

    // Fill local variables.
    mTable = mFiji.openTable("user");
    mReader = mTable.openTableReader();
  }

  @After
  public final void cleanupEnvironment() throws IOException {
    mReader.close();
    mTable.release();
  }

  @Test
  public void testScanner() throws Exception {
    final FijiDataRequest request = FijiDataRequest.create("info", "name");
    final FijiRowScanner scanner = getRowScanner(mTable, mReader, request);
    final Iterator<FijiRowData> iterator = scanner.iterator();

    final String actual1 = iterator.next().getValue("info", "name", 1L).toString();
    final String actual2 = iterator.next().getValue("info", "name", 1L).toString();

    assertEquals("bar-val", actual1);
    assertEquals("foo-val", actual2);

    ResourceUtils.closeOrLog(scanner);
  }

  @Test
  public void testScannerOptionsStart() throws Exception {
    final FijiDataRequest request = FijiDataRequest.create("info", "name");

    final EntityId startRow = mTable.getEntityId("bar");
    final FijiRowScanner scanner = mReader.getScanner(
        request, new FijiScannerOptions().setStartRow(startRow));
    final Iterator<FijiRowData> iterator = scanner.iterator();

    final String first = iterator.next().getValue("info", "name", 1L).toString();
    assertEquals("bar-val", first);

    ResourceUtils.closeOrLog(scanner);
  }
}

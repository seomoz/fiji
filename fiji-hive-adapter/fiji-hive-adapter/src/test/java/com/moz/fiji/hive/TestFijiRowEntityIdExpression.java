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

package com.moz.fiji.hive;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.moz.fiji.hive.io.FijiRowDataWritable;
import com.moz.fiji.schema.EntityId;
import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiClientTest;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiRowData;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.FijiTableReader;
import com.moz.fiji.schema.FijiTableWriter;
import com.moz.fiji.schema.layout.FijiTableLayouts;

/**
 * Tests for parsing and evaluation of FijiRowExpressions.
 */
public class TestFijiRowEntityIdExpression extends FijiClientTest {
  private EntityId mEntityId;
  private Fiji mFiji;
  private FijiTable mTable;
  private FijiTableReader mReader;

  @Before
  public final void setupFijiInstance() throws IOException {
    // Sets up an instance with data at multiple timestamps to utilize the different row
    // expression types.
    mFiji = getFiji();
    mFiji.createTable(FijiTableLayouts.getLayout(FijiTableLayouts.FORMATTED_RKF));
    mTable = mFiji.openTable("table");
    mEntityId = mTable.getEntityId("dummy", "str1", "str2", 1, 1500L);
    final FijiTableWriter writer = mTable.openTableWriter();
    try {
      // Regular column family
      writer.put(mEntityId, "family", "column", 1L, "a");
    } finally {
      writer.close();
    }
    mReader = mTable.openTableReader();
  }

  @After
  public final void teardownFijiInstance() throws IOException {
    mReader.close();
    mTable.release();
    mFiji.deleteTable("table");
  }

  @Test
  public void testEntityIdExpression() throws IOException {
    runEntityIdTest(":entity_id", TypeInfos.ENTITY_ID, "['dummy', 'str1', 'str2', 1, 1500]");
  }

  @Test
  public void testEntityIdStringComponentExpression() throws IOException {
    runEntityIdTest(":entity_id[0]", TypeInfos.ENTITY_ID_STRING_COMPONENT, "dummy");
  }

  @Test
  public void testEntityIdIntComponentExpression() throws IOException {
    runEntityIdTest(":entity_id[3]", TypeInfos.ENTITY_ID_INT_COMPONENT, 1);
  }

  @Test
  public void testEntityIdLongComponentExpression() throws IOException {
    runEntityIdTest(":entity_id[4]", TypeInfos.ENTITY_ID_LONG_COMPONENT, 1500L);
  }

  private void runEntityIdTest(String expression, TypeInfo hiveType, Object expectedResult)
      throws IOException {
    final FijiRowExpression fijiRowExpression = new FijiRowExpression(expression, hiveType);

    // Test that the FijiDataRequest was constructed correctly
    final FijiDataRequest fijiDataRequest = fijiRowExpression.getDataRequest();
    assertEquals(0, fijiDataRequest.getColumns().size());

    // Test that the data returned from this request is decoded properly
    FijiRowData fijiRowData = mReader.get(mEntityId, fijiDataRequest);
    FijiRowDataWritable fijiRowDataWritable = new FijiRowDataWritable(fijiRowData, mReader);
    Object result = fijiRowExpression.evaluate(fijiRowDataWritable);
    assertEquals(expectedResult, result);
  }
}

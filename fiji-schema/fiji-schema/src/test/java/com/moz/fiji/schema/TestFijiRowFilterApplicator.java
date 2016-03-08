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

package com.moz.fiji.schema;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.junit.Before;
import org.junit.Test;

import com.moz.fiji.schema.filter.FijiRowFilter;
import com.moz.fiji.schema.filter.FijiRowFilterApplicator;
import com.moz.fiji.schema.filter.FijiRowFilterDeserializer;
import com.moz.fiji.schema.hbase.HBaseColumnName;
import com.moz.fiji.schema.impl.DefaultFijiCellEncoderFactory;
import com.moz.fiji.schema.impl.hbase.HBaseDataRequestAdapter;
import com.moz.fiji.schema.layout.CellSpec;
import com.moz.fiji.schema.layout.HBaseColumnNameTranslator;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.layout.FijiTableLayouts;
import com.moz.fiji.schema.util.ScanEquals;

public class TestFijiRowFilterApplicator extends FijiClientTest {
  /** The layout of our test table. */
  private FijiTableLayout mTableLayout;

  /** Translates fiji column names into hbase byte arrays. */
  private HBaseColumnNameTranslator mColumnNameTranslator;

  /** Encodes fiji cells into HBase cells. */
  private FijiCellEncoder mCellEncoder;

  /** The filter returned by the MyFijiRowFilter.toHBaseFilter(). */
  private Filter mHBaseFilter;

  @Before
  public void setupTests() throws IOException {
    mTableLayout =
        FijiTableLayouts.getTableLayout(FijiTableLayouts.SIMPLE_UPDATE_NEW_COLUMN);
    getFiji().createTable(mTableLayout.getDesc());

    mColumnNameTranslator = HBaseColumnNameTranslator.from(mTableLayout);
    final CellSpec cellSpec = mTableLayout.getCellSpec(FijiColumnName.create("family", "new"))
        .setSchemaTable(getFiji().getSchemaTable());
    mCellEncoder = DefaultFijiCellEncoderFactory.get().create(cellSpec);

    mHBaseFilter = new SingleColumnValueFilter(
        Bytes.toBytes("family"), Bytes.toBytes("new"), CompareOp.NO_OP, new byte[0]);
  }

  /**
   * A dumb fiji row filter that doesn't return anything useful, but makes sure that we
   * can use the context object to translate between fiji objects and their hbase counterparts.
   */
  public class MyFijiRowFilter extends FijiRowFilter implements FijiRowFilterDeserializer {
    @Override
    public FijiDataRequest getDataRequest() {
      return FijiDataRequest.create("family", "new");
    }

    @Override
    public Filter toHBaseFilter(Context context) throws IOException {
      // Make sure we can translate correctly between fiji objects and their HBase counterparts.
      assertArrayEquals("Row key not translated correctly by FijiRowFilter.Context",
          Bytes.toBytes("foo"), context.getHBaseRowKey("foo"));

      final FijiColumnName column = FijiColumnName.create("family", "new");
      final HBaseColumnName hbaseColumn = context.getHBaseColumnName(column);
      final HBaseColumnName expected = mColumnNameTranslator.toHBaseColumnName(column);
      assertArrayEquals("Family name not translated correctly by FijiRowFilter.Context",
          expected.getFamily(), hbaseColumn.getFamily());
      assertArrayEquals("Qualifier name not translated correctly by FijiRowFilter.Context",
          expected.getQualifier(), hbaseColumn.getQualifier());
      final DecodedCell<Integer> fijiCell =
          new DecodedCell<Integer>(Schema.create(Schema.Type.INT), Integer.valueOf(42));
      assertArrayEquals("Cell value not translated correctly by FijiRowFilter.Context",
          mCellEncoder.encode(fijiCell),
          context.getHBaseCellValue(column, fijiCell));

      return mHBaseFilter;
    }

    @Override
    protected JsonNode toJsonNode() {
      return JsonNodeFactory.instance.nullNode();
    }

    @Override
    protected Class<? extends FijiRowFilterDeserializer> getDeserializerClass() {
      return getClass();
    }

    @Override
    public FijiRowFilter createFromJson(JsonNode root) {
      return this;
    }
  }

  @Test
  public void testApplyToScan() throws Exception {
    // Initialize a scan object with some requested data.
    final FijiDataRequest priorDataRequest = FijiDataRequest.create("family", "column");
    final Scan actualScan =
        new HBaseDataRequestAdapter(priorDataRequest, mColumnNameTranslator).toScan(mTableLayout);

    // Construct a row filter and apply it to the existing scan.
    final FijiRowFilter rowFilter = new MyFijiRowFilter();
    final FijiRowFilterApplicator applicator =
        FijiRowFilterApplicator.create(rowFilter, mTableLayout, getFiji().getSchemaTable());
    applicator.applyTo(actualScan);

    // After filter application, expect the scan to also have the column requested by the filter.
    final Scan expectedScan =
        new HBaseDataRequestAdapter(
            priorDataRequest.merge(rowFilter.getDataRequest()), mColumnNameTranslator)
            .toScan(mTableLayout);
    expectedScan.setFilter(mHBaseFilter);
    assertEquals(expectedScan.toString(), actualScan.toString());
    assertTrue(new ScanEquals(expectedScan).matches(actualScan));
  }
}

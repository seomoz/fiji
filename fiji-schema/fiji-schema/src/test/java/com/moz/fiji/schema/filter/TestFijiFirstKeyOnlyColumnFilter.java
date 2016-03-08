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

package com.moz.fiji.schema.filter;

import static junit.framework.Assert.assertEquals;

import org.apache.hadoop.hbase.HConstants;
import org.junit.Test;

import com.moz.fiji.schema.EntityId;
import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiClientTest;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiDataRequestBuilder.ColumnsDef;
import com.moz.fiji.schema.FijiRowData;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.FijiTableReader;
import com.moz.fiji.schema.layout.FijiTableLayouts;
import com.moz.fiji.schema.util.InstanceBuilder;

/** Tests for {@link FijiFirstKeyOnlyColumnFilter}. */
public class TestFijiFirstKeyOnlyColumnFilter extends FijiClientTest {

  @Test
  public void testHashCodeAndEquals() {
    FijiFirstKeyOnlyColumnFilter filter1 = new FijiFirstKeyOnlyColumnFilter();
    FijiFirstKeyOnlyColumnFilter filter2 = new FijiFirstKeyOnlyColumnFilter();

    assertEquals(filter1.hashCode(), filter2.hashCode());
    assertEquals(filter1, filter2);
  }

  /**
   * Validate the behavior of the FirstKeyOnly column filter: in particular, applying
   * this filter to one column should not affect other columns.
   */
  @Test
  public void testFirstKeyOnlyColumnFilter() throws Exception {
    final Fiji fiji = new InstanceBuilder(getFiji())
        .withTable(FijiTableLayouts.getLayout(FijiTableLayouts.FULL_FEATURED))
            .withRow("row")
                .withFamily("info")
                    .withQualifier("name")
                        .withValue(1L, "name1")
                        .withValue(2L, "name2")
                    .withQualifier("email")
                        .withValue(1L, "email1")
                        .withValue(2L, "email2")
        .build();
    final FijiTable table = fiji.openTable("user");
    try {
      final FijiTableReader reader = table.openTableReader();
      try {
        final EntityId eid = table.getEntityId("row");

        // Make sure we have 2 versions in columns info:name and in info:email.
        {
          final FijiDataRequest dataRequest = FijiDataRequest.builder()
              .addColumns(ColumnsDef.create()
                  .withMaxVersions(HConstants.ALL_VERSIONS)
                  .addFamily("info"))
              .build();
          final FijiRowData row = reader.get(eid, dataRequest);
          assertEquals(2, row.getValues("info", "name").size());
          assertEquals(2, row.getValues("info", "email").size());
        }

        // Test FirstKeyOnly filter when applied on the group-type family.
        {
          final FijiDataRequest dataRequest = FijiDataRequest.builder()
              .addColumns(ColumnsDef.create()
                  .withMaxVersions(HConstants.ALL_VERSIONS)
                  .withFilter(new FijiFirstKeyOnlyColumnFilter())
                  .addFamily("info"))
              .build();
          final FijiRowData row = reader.get(eid, dataRequest);
          assertEquals(1, row.getValues("info", "name").size());
          assertEquals(1, row.getValues("info", "email").size());
        }

        // Test FirstKeyOnly filter when applied on a single column.
        // Make sure it doesn't affect other columns.
        {
          final FijiDataRequest dataRequest = FijiDataRequest.builder()
              .addColumns(ColumnsDef.create()
                  .withMaxVersions(HConstants.ALL_VERSIONS)
                  .withFilter(new FijiFirstKeyOnlyColumnFilter())
                  .add("info", "name"))
              .addColumns(ColumnsDef.create()
                  .withMaxVersions(HConstants.ALL_VERSIONS)
                  .add("info", "email"))
              .build();
          final FijiRowData row = reader.get(eid, dataRequest);
          assertEquals(1, row.getValues("info", "name").size());
          assertEquals(2, row.getValues("info", "email").size());
        }

        // Test FirstKeyOnly filter when applied on a single column.
        // Make sure it doesn't affect other columns.
        {
          final FijiDataRequest dataRequest = FijiDataRequest.builder()
              .addColumns(ColumnsDef.create()
                  .withMaxVersions(HConstants.ALL_VERSIONS)
                  .add("info", "name"))
              .addColumns(ColumnsDef.create()
                  .withMaxVersions(HConstants.ALL_VERSIONS)
                  .withFilter(new FijiFirstKeyOnlyColumnFilter())
                  .add("info", "email"))
              .build();
          final FijiRowData row = reader.get(eid, dataRequest);
          assertEquals(2, row.getValues("info", "name").size());
          assertEquals(1, row.getValues("info", "email").size());
        }

      } finally {
        reader.close();
      }
    } finally {
      table.release();
    }
  }
}

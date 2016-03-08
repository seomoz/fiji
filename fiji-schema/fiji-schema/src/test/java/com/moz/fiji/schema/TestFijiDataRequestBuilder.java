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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import com.moz.fiji.schema.FijiDataRequestBuilder.ColumnsDef;

public class TestFijiDataRequestBuilder {
  @Test
  public void testBuild() {
    FijiDataRequestBuilder builder = FijiDataRequest.builder();
    builder.newColumnsDef().add("info", "foo");
    FijiDataRequest request = builder.build();

    // We should be able to use FijiDataRequest's create() method to similar effect.
    FijiDataRequest request2 = FijiDataRequest.create("info", "foo");
    assertEquals("Constructions methods make different requests", request, request2);
    assertTrue("Builder doesn't build a new object", request != request2);

    assertNotNull("Missing info:foo!", request.getColumn("info", "foo"));
    assertNull("Got spurious info:missing", request.getColumn("info", "missing"));
    assertNull("Got spurious info: family", request.getColumn("info", null));
  }

  @Test
  public void testBuildMore() {
    FijiDataRequestBuilder builder = FijiDataRequest.builder();
    builder.newColumnsDef().add("info", "foo");
    builder.newColumnsDef().add("info", "bar");
    builder.newColumnsDef().addFamily("map");
    FijiDataRequest request = builder.build();

    // Reuse the column builder.
    FijiDataRequestBuilder builder2 = FijiDataRequest.builder();
    builder2.newColumnsDef().add("info", "foo").add("info", "bar").addFamily("map");
    FijiDataRequest request2 = builder2.build();

    // These should have the same effect.
    assertEquals(request, request2);

    FijiDataRequestBuilder builder3 = FijiDataRequest.builder();
    builder3.withTimeRange(3, 4);
    builder3.newColumnsDef().withMaxVersions(10).add("info", "foo");
    builder3.newColumnsDef().withPageSize(6).add("info", "bar");
    FijiDataRequest request3 = builder3.build();

    assertNotNull("missing the expected column", request3.getColumn("info", "foo"));
    FijiDataRequest.Column foo3 = request3.getColumn("info", "foo");
    FijiDataRequest.Column bar3 = request3.getColumn("info", "bar");
    assertEquals("Wrong maxVersions for info:foo", 10, foo3.getMaxVersions());
    assertEquals("Wrong pageSize for info:foo", 0, foo3.getPageSize());
    assertEquals("Wrong maxVersions for info:bar", 1, bar3.getMaxVersions());
    assertEquals("Wrong pageSize for info:bar", 6, bar3.getPageSize());
    assertEquals("Wrong minTs", 3, request3.getMinTimestamp());
    assertEquals("Wrong maxTs", 4, request3.getMaxTimestamp());
  }

  @Test
  public void testBuildMapFamiliesTwoWays() {
    FijiDataRequestBuilder builder1 = FijiDataRequest.builder();
    builder1.newColumnsDef().addFamily("info");
    FijiDataRequest req1 = builder1.build();

    FijiDataRequestBuilder builder2 = FijiDataRequest.builder();
    builder2.newColumnsDef().add("info", null);
    FijiDataRequest req2 = builder2.build();

    assertEquals("These are equivalent ways of specifying a KDR, with unequal results",
        req1, req2);
  }

  @Test
  public void testNoRedundantColumn() {
    FijiDataRequestBuilder builder = FijiDataRequest.builder();
    try {
      builder.newColumnsDef().add("info", "foo").add("info", "foo");
      fail("Should have thrown exception for redundant add");
    } catch (IllegalArgumentException ise) {
      assertEquals("Duplicate request for column 'info:foo'.", ise.getMessage());
    }
  }

  @Test
  public void testNoRedundantColumnIn2ColBuilders() {
    FijiDataRequestBuilder builder = FijiDataRequest.builder();
    builder.newColumnsDef().add("info", "foo");
    builder.newColumnsDef().add("info", "foo");
    try {
      builder.build();
      fail("An exception should have been thrown.");
    } catch (IllegalStateException ise) {
      assertEquals("Duplicate definition for column 'Column{name=info:foo, max_versions=1}'.",
          ise.getMessage());
    }
  }

  @Test
  public void testNoRedundantColumnWithFamily() {
    FijiDataRequestBuilder builder = FijiDataRequest.builder();
    builder.newColumnsDef().add("info", "foo").addFamily("info");
    try {
      builder.build();
      fail("An exception should have been thrown.");
    } catch (IllegalStateException ise) {
      assertEquals("FijiDataRequest may not simultaneously contain definitions for family 'info' "
          + "and definitions for fully qualified columns in family 'info'.", ise.getMessage());
    }
  }

  @Test
  public void testNoRedundantColumnWithFamilyIn2ColBuilders() {
    FijiDataRequestBuilder builder = FijiDataRequest.builder();
    builder.newColumnsDef().add("info", "foo");
    builder.newColumnsDef().addFamily("info");
    try {
      builder.build();
      fail("An exception should have been thrown.");
    } catch (IllegalStateException ise) {
      assertEquals("FijiDataRequest may not simultaneously contain definitions for family 'info' "
          + "and definitions for fully qualified columns in family 'info'.", ise.getMessage());
    }
  }

  @Test
  public void testNoRedundantColumnWithFamilyReversed() {
    FijiDataRequestBuilder builder = FijiDataRequest.builder();
    builder.newColumnsDef().addFamily("info").add("info", "foo");
    try {
      builder.build();
      fail("An exception should have been thrown.");
    } catch (IllegalStateException ise) {
      assertEquals("FijiDataRequest may not simultaneously contain definitions for family 'info' "
          + "and definitions for fully qualified columns 'info:foo'.", ise.getMessage());
    }
  }

  @Test
  public void testNoRedundantColumnWithFamilyIn2ColBuildersReversed() {
    FijiDataRequestBuilder builder = FijiDataRequest.builder();
    builder.newColumnsDef().addFamily("info");
    builder.newColumnsDef().add("info", "foo");
    try {
      builder.build();
      fail("An exception should have been thrown.");
    } catch (IllegalStateException ise) {
      assertEquals("FijiDataRequest may not simultaneously contain definitions for family 'info' "
          + "and definitions for fully qualified columns 'info:foo'.", ise.getMessage());
    }
  }

  @Test
  public void testNoNegativeMaxVer() {
    FijiDataRequestBuilder builder = FijiDataRequest.builder();
    try {
      builder.newColumnsDef().withMaxVersions(-5).addFamily("info");
      fail("An exception should have been thrown.");
    } catch (IllegalArgumentException iae) {
      assertEquals("Maximum number of versions must be strictly positive, but got: -5",
          iae.getMessage());
    }
  }

  @Test
  public void testNoNegativePageSize() {
    FijiDataRequestBuilder builder = FijiDataRequest.builder();
    try {
      builder.newColumnsDef().withPageSize(-5).addFamily("info");
      fail("An exception should have been thrown.");
    } catch (IllegalArgumentException iae) {
      assertEquals("Page size must be 0 (disabled) or positive, but got: null",
          iae.getMessage());
    }
  }

  @Test
  public void testNoZeroMaxVersions() {
    FijiDataRequestBuilder builder = FijiDataRequest.builder();
    try {
      builder.newColumnsDef().withMaxVersions(0).addFamily("info");
      fail("An exception should have been thrown.");
    } catch (IllegalArgumentException iae) {
      assertEquals("Maximum number of versions must be strictly positive, but got: 0",
          iae.getMessage());
    }
  }

  @Test
  public void testZeroPageSizeOk() {
    FijiDataRequestBuilder builder = FijiDataRequest.builder();
    builder.newColumnsDef().withPageSize(0).addFamily("info");
    builder.build();
  }

  @Test
  public void testEmptyOk() {
    assertNotNull(FijiDataRequest.builder().build());
  }

  @Test
  public void testNoSettingMaxVerTwice() {
    FijiDataRequestBuilder builder = FijiDataRequest.builder();
    try {
      builder.newColumnsDef().withMaxVersions(2).withMaxVersions(3).addFamily("info");
      fail("An exception should have been thrown.");
    } catch (IllegalStateException ise) {
      assertEquals("Cannot set max versions to 3, max versions already set to 2.",
          ise.getMessage());
    }
  }

  @Test
  public void testNoSettingPageSizeTwice() {
    FijiDataRequestBuilder builder = FijiDataRequest.builder();
    try {
      builder.newColumnsDef().withPageSize(2).withPageSize(3).addFamily("info");
      fail("An exception should have been thrown.");
    } catch (IllegalStateException ise) {
      assertEquals("Cannot set page size to 3, page size already set to 2.",
          ise.getMessage());
    }
  }

  @Test
  public void testNoSettingTimeRangeTwice() {
    FijiDataRequestBuilder builder = FijiDataRequest.builder();
    try {
      builder.withTimeRange(2, 3).withTimeRange(6, 9);
      fail("An exception should have been thrown.");
    } catch (IllegalStateException ise) {
      assertEquals("Cannot set time range more than once.", ise.getMessage());
    }
  }

  @Test
  public void testNoPropertiesAfterAdd() {
    FijiDataRequestBuilder builder = FijiDataRequest.builder();
    try {
      builder.newColumnsDef().add("info", "foo").withMaxVersions(5);
      fail("An exception should have been thrown.");
    } catch (IllegalStateException ise) {
      assertEquals(
          "Properties of the columns builder cannot be changed once columns are assigned to it.",
          ise.getMessage());
    }
  }

  @Test
  public void testNoAddAfterBuild() {
    FijiDataRequestBuilder builder = FijiDataRequest.builder();
    builder.newColumnsDef().withMaxVersions(5).add("info", "foo");
    builder.build();
    try {
      builder.newColumnsDef().add("info", "bar");
      fail("An exception should have been thrown.");
    } catch (IllegalStateException ise) {
      assertEquals("FijiDataRequest builder cannot be used after build() is invoked.",
          ise.getMessage());
    }
  }

  @Test
  public void testNoPropertiesAfterBuild() {
    FijiDataRequestBuilder builder = FijiDataRequest.builder();
    FijiDataRequestBuilder.ColumnsDef columns = builder.newColumnsDef();
    columns.withMaxVersions(5).add("info", "foo");
    builder.build();
    try {
      columns.add("info", "bar");
      fail("An exception should have been thrown.");
    } catch (IllegalStateException ise) {
      assertEquals("Cannot add more columns to this ColumnsDef after build() has been called.",
          ise.getMessage());
    }
  }

  @Test
  public void testCannotBuildTwoTimes() {
    FijiDataRequestBuilder builder = FijiDataRequest.builder();
    FijiDataRequestBuilder.ColumnsDef columns = builder.newColumnsDef();
    columns.withMaxVersions(5).add("info", "foo");
    builder.build();
    try {
      builder.build();
      fail("An exception should have been thrown.");
    } catch (IllegalStateException ise) {
      assertEquals("FijiDataRequest builder cannot be used after build() is invoked.",
          ise.getMessage());
    }
  }

  @Test
  public void testChaining() {
    final FijiDataRequest kdr = FijiDataRequest.builder()
        .addColumns(ColumnsDef.create().withMaxVersions(10).addFamily("info"))
        .addColumns(ColumnsDef.create().add("users", "details"))
        .build();
    assertEquals(2, kdr.getColumns().size());
    final FijiDataRequest.Column infoCol = kdr.getColumn("info", null);
    assertEquals("info", infoCol.getFamily());
    assertEquals(null, infoCol.getQualifier());
    final FijiDataRequest.Column usersDetailsCol = kdr.getColumn("users", "details");
    assertEquals("users", usersDetailsCol.getFamily());
    assertEquals("details", usersDetailsCol.getQualifier());
  }

  @Test
  public void testNoColumnsDefReuse() {
    final ColumnsDef cols = ColumnsDef.create().withMaxVersions(10).addFamily("info");
    FijiDataRequest.builder().addColumns(cols).build();  // seals cols
    try {
      FijiDataRequest.builder().addColumns(cols).build();  // fails as cols is already sealed.
      fail("An exception should have been thrown.");
    } catch (IllegalStateException ise) {
      assertEquals("ColumnsDef is sealed.  This usually indicates that the ColumnsDef has been "
          + "used in the construction of another FijiDataRequest already.", ise.getMessage());
    }
  }
}

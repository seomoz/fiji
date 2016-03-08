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
package com.moz.fiji.schema.util;

import java.io.Closeable;
import java.io.IOException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiClientTest;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiIOException;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.FijiTableReader;
import com.moz.fiji.schema.FijiTableReaderPool;
import com.moz.fiji.schema.FijiTableWriter;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.layout.FijiTableLayouts;
import com.moz.fiji.schema.util.ResourceUtils.CompoundException;
import com.moz.fiji.schema.util.ResourceUtils.DoAndClose;
import com.moz.fiji.schema.util.ResourceUtils.DoAndRelease;
import com.moz.fiji.schema.util.ResourceUtils.WithFiji;
import com.moz.fiji.schema.util.ResourceUtils.WithFijiTable;
import com.moz.fiji.schema.util.ResourceUtils.WithFijiTableReader;
import com.moz.fiji.schema.util.ResourceUtils.WithFijiTableWriter;

public class TestResourceUtils extends FijiClientTest {
  private static final String TABLE_NAME = "row_data_test_table";
  private static final FijiDataRequest FAMILY_QUAL0_R = FijiDataRequest.create("family", "qual0");

  private FijiTable mTable = null;

  @Before
  public void setupTestResourceUtils() throws IOException {
    final FijiTableLayout layout = FijiTableLayout.newLayout(
        FijiTableLayouts.getLayout(FijiTableLayouts.ROW_DATA_TEST));
    new InstanceBuilder(getFiji())
        .withTable("row_data_test_table", layout)
            .withRow("foo")
                .withFamily("family")
                    .withQualifier("qual0").withValue(5L, "foo-val")
                    .withQualifier("qual1").withValue(5L, "foo-val")
                    .withQualifier("qual2").withValue(5L, "foo@val.com")
                .withFamily("map")
                    .withQualifier("qualifier").withValue(5L, 1)
                    .withQualifier("qualifier1").withValue(5L, 2)
            .withRow("bar")
                .withFamily("family")
                    .withQualifier("qual0").withValue(5L, "bar-val")
                    .withQualifier("qual2").withValue(5L, "bar@val.com")
        .build();
    mTable = getFiji().openTable(TABLE_NAME);
  }

  @After
  public void cleanupTestResourceUtils() throws IOException {
    mTable.release();
  }

  @Test
  public void testWithFijiTableReader() throws Exception {
    Assert.assertEquals("foo-val", new WithFijiTableReader<String>(mTable.getURI()) {
      @Override
      public String run(final FijiTableReader fijiTableReader) throws Exception {
        return fijiTableReader.get(
            mTable.getEntityId("foo"),
            FAMILY_QUAL0_R
        ).getMostRecentValue("family", "qual0").toString();
      }
    }.eval());

    Assert.assertEquals("foo-val", new WithFijiTableReader<String>(mTable) {
      @Override
      public String run(final FijiTableReader fijiTableReader) throws Exception {
        return fijiTableReader.get(
            mTable.getEntityId("foo"),
            FAMILY_QUAL0_R
        ).getMostRecentValue("family", "qual0").toString();
      }
    }.eval());

    final FijiTableReaderPool pool = FijiTableReaderPool.Builder.create()
        .withReaderFactory(mTable.getReaderFactory())
        .build();
    try {
      Assert.assertEquals("foo-val", new WithFijiTableReader<String>(pool) {
        @Override
        public String run(final FijiTableReader fijiTableReader) throws Exception {
          return fijiTableReader.get(
              mTable.getEntityId("foo"),
              FAMILY_QUAL0_R
          ).getMostRecentValue("family", "qual0").toString();
        }
      }.eval());
    } finally {
      pool.close();
    }
  }

  @Test
  public void testWithFijiTableWriter() throws Exception {
    final String expected = "expected";
    Assert.assertEquals(expected, new WithFijiTableWriter<String>(mTable.getURI()) {
      @Override
      public String run(final FijiTableWriter fijiTableWriter) throws Exception {
        fijiTableWriter.put(mTable.getEntityId("bar"), "family", "qual0", expected);
        final FijiTableReader reader = mTable.openTableReader();
        try {
          return reader.get(
              mTable.getEntityId("bar"),
              FAMILY_QUAL0_R
          ).getMostRecentValue("family", "qual0").toString();
        } finally {
          reader.close();
        }
      }
    }.eval());

    Assert.assertEquals(expected, new WithFijiTableWriter<String>(mTable) {
      @Override
      public String run(final FijiTableWriter fijiTableWriter) throws Exception {
        fijiTableWriter.put(mTable.getEntityId("bar"), "family", "qual0", expected);
        final FijiTableReader reader = mTable.openTableReader();
        try {
          return reader.get(
              mTable.getEntityId("bar"),
              FAMILY_QUAL0_R
          ).getMostRecentValue("family", "qual0").toString();
        } finally {
          reader.close();
        }
      }
    }.eval());
  }

  @Test
  public void testWithFijiTable() throws Exception {
    Assert.assertEquals(TABLE_NAME, new WithFijiTable<String>(mTable.getURI()) {
      @Override
      public String run(final FijiTable fijiTable) throws Exception {
        return fijiTable.getName();
      }
    }.eval());

    Assert.assertEquals(TABLE_NAME, new WithFijiTable<String>(getFiji(), TABLE_NAME) {
      @Override
      public String run(final FijiTable fijiTable) throws Exception {
        return fijiTable.getName();
      }
    }.eval());
  }

  @Test
  public void testWithFiji() throws Exception {
    Assert.assertEquals(getFiji().getURI(), new WithFiji<FijiURI>(getFiji().getURI()) {
      @Override
      public FijiURI run(final Fiji fiji) throws Exception {
        return fiji.getURI();
      }
    }.eval());
  }

  @Test
  public void testDoAndClose() throws Exception {
    Assert.assertEquals("foo-val", new DoAndClose<FijiTableReader, String>() {
      @Override
      public FijiTableReader openResource() throws Exception {
        return mTable.openTableReader();
      }

      @Override
      public String run(final FijiTableReader fijiTableReader) throws Exception {
        try {
          return fijiTableReader.get(
              mTable.getEntityId("foo"),
              FAMILY_QUAL0_R
          ).getMostRecentValue("family", "qual0").toString();
        } catch (IOException e) {
          throw new FijiIOException(e);
        }
      }
    }.eval());
  }

  @Test
  public void testDoAndRelease() throws Exception {
    Assert.assertEquals(TABLE_NAME, new DoAndRelease<FijiTable, String>() {
      @Override
      public FijiTable openResource() throws Exception {
        return getFiji().openTable(TABLE_NAME);
      }

      @Override
      public String run(final FijiTable fijiTable) throws Exception {
        return fijiTable.getName();
      }
    }.eval());
  }

  private static final class BrokenCloseable implements Closeable {
    @Override
    public void close() throws IOException {
      throw new IOException("close");
    }
  }

  @Test
  public void testExceptions() throws Exception {
    try {
      new DoAndClose<BrokenCloseable, String>() {

        @Override
        protected BrokenCloseable openResource() throws Exception {
          return new BrokenCloseable();
        }

        @Override
        protected String run(final BrokenCloseable brokenCloseable) throws Exception {
          throw new IOException("run");
        }
      }.eval();
    } catch (CompoundException ce) {
      Assert.assertEquals("Exception was throw while cleaning up resources after another exception "
          + "was thrown.: first exception: run second exception: close", ce.getMessage());
    }
  }
}

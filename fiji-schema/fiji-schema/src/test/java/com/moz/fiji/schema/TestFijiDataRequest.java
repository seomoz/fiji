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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.junit.Test;

import com.moz.fiji.schema.FijiDataRequestBuilder.ColumnsDef;
import com.moz.fiji.schema.avro.TestRecord1;
import com.moz.fiji.schema.layout.ColumnReaderSpec;

/** Tests for FijiDataRequest and FijiDataRequestBuilder. */
public class TestFijiDataRequest {

  /** Checks that FijiDataRequest serializes and deserializes correctly. */
  @Test
  public void testSerializability() throws Exception {
    final FijiDataRequest expected = FijiDataRequest.builder()
        .addColumns(ColumnsDef.create()
            .add("foo", "bar1")
            .add("foo", "bar2")
            .add("foo", "bar3", ColumnReaderSpec.bytes())
            .add("foo", "bar4")
        )
        .build();

    ByteArrayOutputStream byteOutput = new ByteArrayOutputStream();
    new ObjectOutputStream(byteOutput).writeObject(expected);

    byte[] bytes = byteOutput.toByteArray();

    ByteArrayInputStream byteInput = new ByteArrayInputStream(bytes);
    FijiDataRequest actual = (FijiDataRequest) (new ObjectInputStream(byteInput).readObject());

    assertEquals(expected, actual);
  }

  /** Checks that FijiDataRequest with schema overrides serializes and deserializes correctly. */
  @Test
  public void testSchemaOverrideSerializability() throws Exception {
    final FijiColumnName columnName = FijiColumnName.create("family", "empty");
    final FijiDataRequest overrideRequest = FijiDataRequest.builder()
        .addColumns(ColumnsDef.create()
            .add(columnName, ColumnReaderSpec.avroReaderSchemaSpecific(TestRecord1.class))).build();
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(overrideRequest);
    final ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    final ObjectInputStream ois = new ObjectInputStream(bais);
    final FijiDataRequest deserializedRequest = (FijiDataRequest) ois.readObject();
    assertEquals(overrideRequest, deserializedRequest);
  }

  @Test
  public void testColumnRequestEquals() {
    FijiDataRequestBuilder builder = FijiDataRequest.builder();
    builder.newColumnsDef().add("foo", "bar");
    FijiDataRequest req0 = builder.build();

    builder = FijiDataRequest.builder();
    builder.newColumnsDef().add("foo", "bar");
    FijiDataRequest req1 = builder.build();
    assertTrue(req0 != req1);
    assertEquals(req0, req0);
    FijiDataRequest.Column foo0 = req0.getColumn("foo", "bar");
    FijiDataRequest.Column foo1 = req1.getColumn("foo", "bar");
    assertEquals(foo0, foo0);
    assertEquals(foo0, foo1);
    assertEquals(foo1, foo0);

    builder = FijiDataRequest.builder();
    builder.newColumnsDef().withMaxVersions(2).add("foo", "bar");
    builder.newColumnsDef().add("foo", "baz");
    FijiDataRequest req2 = builder.build();
    FijiDataRequest.Column foo2 = req2.getColumn("foo", "bar");
    assertThat(new Object(), is(not((Object) foo2)));
    assertFalse(foo0.equals(foo2));
    assertFalse(foo2.equals(foo0));
    assertThat(foo1, is(not(foo2)));

    FijiDataRequest.Column foo3 = req2.getColumn("foo", "baz");
    assertFalse(foo0.equals(foo3));
    assertThat(foo1, is(not(foo3)));
  }

  @Test
  public void testDataRequestEquals() {
    FijiDataRequestBuilder builder0 = FijiDataRequest.builder()
        .withTimeRange(3L, 4L);
    builder0.newColumnsDef().withMaxVersions(2).addFamily("foo");
    builder0.newColumnsDef().withMaxVersions(5).add("bar", "baz");
    FijiDataRequest request0 = builder0.build();

    FijiDataRequestBuilder builder1 = FijiDataRequest.builder()
        .withTimeRange(3L, 4L);
    builder1.newColumnsDef().withMaxVersions(2).addFamily("foo");
    builder1.newColumnsDef().withMaxVersions(5).add("bar", "baz");
    FijiDataRequest request1 = builder1.build();

    FijiDataRequestBuilder builder2 = FijiDataRequest.builder()
        .withTimeRange(3L, 4L);
    builder2.newColumnsDef().withMaxVersions(2).addFamily("foo");
    builder2.newColumnsDef().withMaxVersions(5).add("car", "bot");
    FijiDataRequest request2 = builder2.build();

    FijiDataRequestBuilder builder3 = FijiDataRequest.builder()
        .withTimeRange(3L, 4L);
    builder3.newColumnsDef().withMaxVersions(2).addFamily("foo");
    builder3.newColumnsDef().withMaxVersions(3).add("car", "bot");
    FijiDataRequest request3 = builder3.build();

    assertEquals(request0, request1);
    assertThat(new Object(), is(not((Object) request0)));
    assertThat(request0, is(not(request2)));
    assertThat(request2, is(not(request3)));
  }

  @Test
  public void testMerge() {
    FijiDataRequestBuilder builder1 = FijiDataRequest.builder().withTimeRange(3, 4);
    builder1.newColumnsDef().withMaxVersions(2).add("foo", "bar");
    FijiDataRequest first = builder1.build();

    FijiDataRequestBuilder builder2 = FijiDataRequest.builder().withTimeRange(2, 4);
    builder2.newColumnsDef().add("baz", "bot");
    builder2.newColumnsDef().withMaxVersions(6).add("foo", "bar");
    FijiDataRequest second = builder2.build();

    FijiDataRequest merged = first.merge(second);
    assertTrue("merge() cannot mutate the object in place", first != merged);

    FijiDataRequest.Column fooBarColumnRequest = merged.getColumn("foo", "bar");
    assertNotNull("Missing column foo:bar from merged request", fooBarColumnRequest);
    assertEquals("Max versions was not increased", 6, fooBarColumnRequest.getMaxVersions());
    assertEquals("Time range was not extended", 2L, merged.getMinTimestamp());
    assertEquals(4L, merged.getMaxTimestamp());

    FijiDataRequest.Column bazBotColumnRequest = merged.getColumn("baz", "bot");
    assertNotNull("Missing column from merged-in request", bazBotColumnRequest);

    FijiDataRequest symmetricMerged = second.merge(first);
    assertEquals("Merge must be symmetric", merged, symmetricMerged);
  }

  @Test
  public void testInvalidColumnSpec() {
    // The user really wants 'builder.columns().add("family", "qualifier")'.
    // This will throw an exception.
    try {
      FijiDataRequest.builder().newColumnsDef().addFamily("family:qualifier");
      fail("An exception should have been thrown.");
    } catch (FijiInvalidNameException kine) {
      assertEquals(
          "Invalid family name: family:qualifier Name must match pattern: [a-zA-Z_][a-zA-Z0-9_]*",
          kine.getMessage());
    }
  }

  @Test
  public void testPageSize() {
    final FijiDataRequestBuilder builder1 = FijiDataRequest.builder();
    builder1.newColumnsDef().withPageSize(1).add("foo", "bar");
    final FijiDataRequest first = builder1.build();

    final FijiDataRequestBuilder builder2 = FijiDataRequest.builder();
    builder2.newColumnsDef().add("foo", "bar");
    final FijiDataRequest second = builder2.build();

    assertThat(first, is(not(second)));
    assertFalse(first.equals(second));
    assertFalse(second.equals(first));
  }

  @Test
  public void testPageSizeMerge() {
    // Page size should merge to the smallest value.

    final FijiDataRequestBuilder builder1 = FijiDataRequest.builder();
    builder1.newColumnsDef().withPageSize(1).add("foo", "bar");
    final FijiDataRequest first = builder1.build();

    final FijiDataRequestBuilder builder2 = FijiDataRequest.builder();
    builder2.newColumnsDef().withPageSize(3).add("foo", "bar");
    final FijiDataRequest second = builder2.build();

    assertEquals("Unexpected page size for 'first'",
        1, first.getColumn("foo", "bar").getPageSize());
    assertEquals("Unexpected page size for 'second'",
        3, second.getColumn("foo", "bar").getPageSize());

    final FijiDataRequest merge1 = first.merge(second);
    final FijiDataRequest merge2 = second.merge(first);
    assertEquals("Merged results should be symmetric", merge1, merge2);
    assertEquals("Unexpected merged page size",
        1, merge1.getColumn("foo", "bar").getPageSize());
  }

  @Test
  public void testPageSizeMergeWithZero() {
    // ... unless the smallest value is zero, in which case we go with the
    // non-zero value.

    final FijiDataRequestBuilder builder1 = FijiDataRequest.builder();
    builder1.newColumnsDef().withPageSize(4).add("foo", "bar");
    final FijiDataRequest first = builder1.build();

    final FijiDataRequestBuilder builder2 = FijiDataRequest.builder();
    builder2.newColumnsDef().add("foo", "bar");
    final FijiDataRequest second = builder2.build();

    assertEquals("Unexpected page size for 'first'",
        4, first.getColumn("foo", "bar").getPageSize());
    assertEquals("Unexpected page size for 'second'",
        0, second.getColumn("foo", "bar").getPageSize());

    final FijiDataRequest merge1 = first.merge(second);
    final FijiDataRequest merge2 = second.merge(first);
    assertEquals("Merged results should be symmetric", merge1, merge2);
    assertEquals("Unexpected merged page size",
        4, merge1.getColumn("foo", "bar").getPageSize());
  }
}

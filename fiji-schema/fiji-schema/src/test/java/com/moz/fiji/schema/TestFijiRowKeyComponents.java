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

package com.moz.fiji.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import junit.framework.Assert;
import org.junit.Test;

import com.moz.fiji.schema.avro.ComponentType;
import com.moz.fiji.schema.avro.HashSpec;
import com.moz.fiji.schema.avro.RowKeyComponent;
import com.moz.fiji.schema.avro.RowKeyEncoding;
import com.moz.fiji.schema.avro.RowKeyFormat;
import com.moz.fiji.schema.avro.RowKeyFormat2;
import com.moz.fiji.schema.layout.FijiTableLayouts;

public class TestFijiRowKeyComponents extends FijiClientTest {
  private RowKeyFormat2 makeFormattedRowKeyFormat() {
    // components of the row key
    final List<RowKeyComponent> components = Lists.newArrayList(
        RowKeyComponent.newBuilder().setName("dummy").setType(ComponentType.STRING).build(),
        RowKeyComponent.newBuilder().setName("str1").setType(ComponentType.STRING).build(),
        RowKeyComponent.newBuilder().setName("str2").setType(ComponentType.STRING).build(),
        RowKeyComponent.newBuilder().setName("anint").setType(ComponentType.INTEGER).build(),
        RowKeyComponent.newBuilder().setName("along").setType(ComponentType.LONG).build());

    // build the row key format
    final RowKeyFormat2 format = RowKeyFormat2.newBuilder().setEncoding(RowKeyEncoding.FORMATTED)
        .setSalt(HashSpec.newBuilder().setHashSize(2).build())
        .setComponents(components)
        .build();

    return format;
  }

  private RowKeyFormat makeRawRowKeyFormat() {
    final RowKeyFormat format = RowKeyFormat.newBuilder().setEncoding(RowKeyEncoding.RAW).build();
    return format;
  }

  @Test
  public void testRawKeys() throws Exception {
    EntityIdFactory factory = EntityIdFactory.getFactory(makeRawRowKeyFormat());
    FijiRowKeyComponents krkc = FijiRowKeyComponents.fromComponents("skimbleshanks");

    assertEquals(factory.getEntityId("skimbleshanks"), factory.getEntityId(krkc));

    // Install a table with a raw format and ensure that the checks work out.
    Fiji fiji = getFiji();
    fiji.createTable(FijiTableLayouts.getLayout(FijiTableLayouts.SIMPLE_UNHASHED));
    FijiTable table = fiji.openTable("table");
    assertEquals(table.getEntityId("skimbleshanks"), factory.getEntityId(krkc));
    assertEquals(table.getEntityId("skimbleshanks"), krkc.getEntityIdForTable(table));
    table.release();
  }

  @Test
  public void testFormattedKeys() throws Exception {
    EntityIdFactory factory = EntityIdFactory.getFactory(makeFormattedRowKeyFormat());
    FijiRowKeyComponents krkc = FijiRowKeyComponents.fromComponents(
        "skimbleshanks",
        "mungojerrie",
        "rumpelteazer",
        5
        /* Last component left as a null */);

    assertEquals(
        factory.getEntityId("skimbleshanks", "mungojerrie", "rumpelteazer", 5, null),
        factory.getEntityId(krkc));

    // Install a table with the same key format and ensure that the checks work out.
    Fiji fiji = getFiji();
    fiji.createTable(FijiTableLayouts.getLayout(FijiTableLayouts.FORMATTED_RKF));
    FijiTable table = fiji.openTable("table");
    assertEquals(
        table.getEntityId("skimbleshanks", "mungojerrie", "rumpelteazer", 5),
        factory.getEntityId(krkc));
    assertEquals(
        table.getEntityId("skimbleshanks", "mungojerrie", "rumpelteazer", 5, null),
        krkc.getEntityIdForTable(table));
    table.release();
  }

  @Test
  public void testEquals() throws Exception {
    FijiRowKeyComponents krkc1 = FijiRowKeyComponents.fromComponents(
        "jennyanydots",
        1,
        null,
        null);
    FijiRowKeyComponents krkc2 = FijiRowKeyComponents.fromComponents(
        "jennyanydots",
        1,
        null,
        null);
    FijiRowKeyComponents krkc3 = FijiRowKeyComponents.fromComponents(
        "jennyanydots",
        1,
        null);
    assertFalse(krkc1 == krkc2);
    assertEquals(krkc1, krkc2);
    assertFalse(krkc1.equals(krkc3));

    // byte[] use a different code path.
    byte[] bytes1 = new byte[]{47};
    byte[] bytes2 = new byte[]{47};
    assertFalse(bytes1.equals(bytes2));
    FijiRowKeyComponents krkc4 = FijiRowKeyComponents.fromComponents(bytes1);
    FijiRowKeyComponents krkc5 = FijiRowKeyComponents.fromComponents(bytes2);
    assertEquals(krkc4, krkc5);
  }

  @Test
  public void testHashCode() throws Exception {
    byte[] bytes1 = new byte[]{47};
    byte[] bytes2 = new byte[]{47};
    assertFalse(bytes1.equals(bytes2));
    FijiRowKeyComponents krkc1 = FijiRowKeyComponents.fromComponents(bytes1);
    FijiRowKeyComponents krkc2 = FijiRowKeyComponents.fromComponents(bytes2);
    assertEquals(krkc1.hashCode(), krkc2.hashCode());
  }

  @Test
  public void testFormattedCompare() {
    List<FijiRowKeyComponents> sorted = ImmutableList.of(
        FijiRowKeyComponents.fromComponents("a", null, null),
        FijiRowKeyComponents.fromComponents("a", 123, 456L),
        FijiRowKeyComponents.fromComponents("a", 456, null));

    List<FijiRowKeyComponents> shuffled = Lists.newArrayList(sorted);
    Collections.shuffle(shuffled);
    Collections.sort(shuffled);

    Assert.assertEquals(sorted, shuffled);
  }

  @Test
  public void testRawCompare() {
    final List<FijiRowKeyComponents> sorted = ImmutableList.of(
        FijiRowKeyComponents.fromComponents(new Object[] {new byte[] {0x00, 0x00}}),
        FijiRowKeyComponents.fromComponents(new Object[] {new byte[] {0x00, 0x01}}),
        FijiRowKeyComponents.fromComponents(new Object[] {new byte[] {0x01, 0x00}}));

    List<FijiRowKeyComponents> shuffled = Lists.newArrayList(sorted);
    Collections.shuffle(shuffled);
    Collections.sort(shuffled);

    Assert.assertEquals(sorted, shuffled);
  }
}

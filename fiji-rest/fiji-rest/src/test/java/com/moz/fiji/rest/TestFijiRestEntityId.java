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

package com.moz.fiji.rest;

import static org.junit.Assert.*;

import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jettison.json.JSONObject;

import org.junit.Test;

import com.moz.fiji.rest.representations.FijiRestEntityId;
import com.moz.fiji.schema.EntityId;
import com.moz.fiji.schema.EntityIdFactory;
import com.moz.fiji.schema.avro.TableLayoutDesc;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.layout.FijiTableLayouts;
import com.moz.fiji.schema.tools.ToolUtils;
import com.moz.fiji.schema.util.Hasher;

public class TestFijiRestEntityId {
  // Unusual UTF-16 string with Cyrillic, Imperial Phoenician, etc.
  private static final String UNUSUAL_STRING_EID = "abcde\b\fa0€ÿҖ𐤇";
  private static final String SINGLE_COMPONENT_EID =
      String.format("[%s]", JSONObject.quote(UNUSUAL_STRING_EID));

  @Test
  public void testShouldWorkWithRKFRawLayout() throws Exception {
    final TableLayoutDesc desc =
        FijiTableLayouts.getLayout("com.moz.fiji/rest/layouts/rkf_raw.json");
    final FijiTableLayout layout = FijiTableLayout.newLayout(desc);
    final EntityIdFactory factory = EntityIdFactory.getFactory(layout);
    final byte[] rowKey = Bytes.toBytes(UNUSUAL_STRING_EID);
    final EntityId originalEid = factory.getEntityIdFromHBaseRowKey(rowKey);
    final FijiRestEntityId restEid1 = FijiRestEntityId.create(originalEid, layout);
    final FijiRestEntityId restEid2 = FijiRestEntityId.create(
        String.format("hbase=%s", Bytes.toStringBinary(rowKey)));
    final FijiRestEntityId restEid3 = FijiRestEntityId.create(
        String.format("hbase_hex=%s", new String(Hex.encodeHex((rowKey)))));

    // Resolved entity id should match origin entity id.
    assertEquals(originalEid, restEid1.resolve(layout));
    assertEquals(originalEid, restEid2.resolve(layout));
    assertEquals(originalEid, restEid3.resolve(layout));
  }

  @Test
  public void testShouldWorkWithRKFHashedLayout() throws Exception {
    final TableLayoutDesc desc =
        FijiTableLayouts.getLayout("com.moz.fiji/rest/layouts/rkf_hashed.json");
    final FijiTableLayout layout = FijiTableLayout.newLayout(desc);
    final EntityIdFactory factory = EntityIdFactory.getFactory(layout);

    // Byte array representations of row keys should work.
    final byte[] rowKey = Bytes.toBytes(UNUSUAL_STRING_EID);
    final EntityId originalEid = factory.getEntityIdFromHBaseRowKey(rowKey);
    final FijiRestEntityId restEid1 = FijiRestEntityId.create(originalEid, layout);
    final FijiRestEntityId restEid2 = FijiRestEntityId.create(
        String.format("hbase=%s", Bytes.toStringBinary(rowKey)));
    final FijiRestEntityId restEid3 = FijiRestEntityId.create(
        String.format("hbase_hex=%s", new String(Hex.encodeHex((rowKey)))));

    // Resolved entity id should match origin entity id.
    assertEquals(originalEid, restEid1.resolve(layout));
    assertEquals(originalEid, restEid2.resolve(layout));
    assertEquals(originalEid, restEid3.resolve(layout));

    // Component representation of entities should work.
    final FijiRestEntityId restEid4 = FijiRestEntityId.createFromUrl(SINGLE_COMPONENT_EID, layout);
    final EntityId resolvedEid = restEid4.resolve(layout);
    final String recoveredComponent = Bytes.toString(
        Bytes.toBytesBinary(
            resolvedEid.toShellString().substring(
                ToolUtils.FIJI_ROW_KEY_SPEC_PREFIX.length())));
    assertEquals(UNUSUAL_STRING_EID, recoveredComponent);
    assertEquals(resolvedEid, restEid4.resolve(layout));
  }

  @Test
  public void testShouldWorkWithRKFHashPrefixedLayout() throws Exception {
    final TableLayoutDesc desc =
        FijiTableLayouts.getLayout("com.moz.fiji/rest/layouts/rkf_hashprefixed.json");
    final FijiTableLayout layout = FijiTableLayout.newLayout(desc);
    final EntityIdFactory factory = EntityIdFactory.getFactory(layout);

    // Byte array representations of row keys should work.
    // Prepend appropriate hashed prefix to UNUSUAL_STRING_EID.
    final byte[] rowKey = Bytes.toBytes(UNUSUAL_STRING_EID);
    final byte[] hash = Hasher.hash(Bytes.toBytes(UNUSUAL_STRING_EID));
    final byte[] hbaseRowKey = new byte[rowKey.length + 2];
    System.arraycopy(hash, 0, hbaseRowKey, 0, 2);
    System.arraycopy(rowKey, 0, hbaseRowKey, 2, rowKey.length);
    final EntityId originalEid = factory.getEntityIdFromHBaseRowKey(hbaseRowKey);
    final FijiRestEntityId restEid1 = FijiRestEntityId.create(originalEid, layout);
    final FijiRestEntityId restEid2 = FijiRestEntityId.create(
        String.format("hbase=%s", Bytes.toStringBinary(hbaseRowKey)));
    final FijiRestEntityId restEid3 = FijiRestEntityId.create(
        String.format("hbase_hex=%s", new String(Hex.encodeHex((hbaseRowKey)))));

    // Resolved entity id should match origin entity id.
    assertEquals(originalEid, restEid1.resolve(layout));
    assertEquals(originalEid, restEid2.resolve(layout));
    assertEquals(originalEid, restEid3.resolve(layout));

    // Component representation of entities should work.
    final FijiRestEntityId restEid4 = FijiRestEntityId.createFromUrl(SINGLE_COMPONENT_EID, layout);
    final EntityId resolvedEid = restEid4.resolve(layout);
    final String recoveredComponent = Bytes.toString(
        Bytes.toBytesBinary(
            resolvedEid.toShellString().substring(
                ToolUtils.FIJI_ROW_KEY_SPEC_PREFIX.length())));
    assertEquals(UNUSUAL_STRING_EID, recoveredComponent);
    assertEquals(resolvedEid, restEid4.resolve(layout));
  }

    @Test
    public void testShouldWorkWithRKF2RawLayout() throws Exception {
      final TableLayoutDesc desc =
          FijiTableLayouts.getLayout("com.moz.fiji/rest/layouts/rkf2_raw.json");
      final FijiTableLayout layout = FijiTableLayout.newLayout(desc);
      final EntityIdFactory factory = EntityIdFactory.getFactory(layout);
      final byte[] rowKey = Bytes.toBytes(UNUSUAL_STRING_EID);
      final EntityId originalEid = factory.getEntityIdFromHBaseRowKey(rowKey);
      final FijiRestEntityId restEid1 = FijiRestEntityId.create(originalEid, layout);
      final FijiRestEntityId restEid2 = FijiRestEntityId.create(
          String.format("hbase=%s", Bytes.toStringBinary(rowKey)));
      final FijiRestEntityId restEid3 = FijiRestEntityId.create(
          String.format("hbase_hex=%s", new String(Hex.encodeHex((rowKey)))));

      // Resolved entity id should match origin entity id.
      assertEquals(originalEid, restEid1.resolve(layout));
      assertEquals(originalEid, restEid2.resolve(layout));
      assertEquals(originalEid, restEid3.resolve(layout));
    }

    @Test
    public void testShouldWorkWithRKF2SuppressedLayout() throws Exception {
      final TableLayoutDesc desc =
          FijiTableLayouts.getLayout("com.moz.fiji/rest/layouts/rkf2_suppressed.json");
      final FijiTableLayout layout = FijiTableLayout.newLayout(desc);

      // Construct complex entity id.
      final String eidString = String.format("[%s,%s,%s,%d,%d]",
          JSONObject.quote(UNUSUAL_STRING_EID),
          JSONObject.quote(UNUSUAL_STRING_EID),
          JSONObject.quote(UNUSUAL_STRING_EID),
          Integer.MIN_VALUE,
          Long.MAX_VALUE);
      final EntityId originalEid = ToolUtils.createEntityIdFromUserInputs(eidString, layout);
      final FijiRestEntityId restEid1 = FijiRestEntityId.create(originalEid, layout);
      final FijiRestEntityId restEid2 = FijiRestEntityId.createFromUrl(eidString, layout);
      final FijiRestEntityId restEid3 = FijiRestEntityId.create(
          String.format("hbase_hex=%s",
              new String(Hex.encodeHex((originalEid.getHBaseRowKey())))));
      final FijiRestEntityId restEid4 = FijiRestEntityId.create(
              String.format("hbase=%s", Bytes.toStringBinary(originalEid.getHBaseRowKey())));

      // Resolved entity id should match origin entity id.
      assertEquals(originalEid, restEid1.resolve(layout));
      assertEquals(originalEid, restEid2.resolve(layout));
      assertEquals(originalEid, restEid3.resolve(layout));
      assertEquals(originalEid, restEid4.resolve(layout));
    }

    @Test
    public void testShouldWorkWithRKF2FormattedLayout() throws Exception {
      final TableLayoutDesc desc =
          FijiTableLayouts.getLayout("com.moz.fiji/rest/layouts/rkf2_suppressed.json");
      final FijiTableLayout layout = FijiTableLayout.newLayout(desc);

      // Construct complex entity id.
      final String eidString = String.format("[%s,%s,%s,%d,%d]",
          JSONObject.quote(UNUSUAL_STRING_EID),
          JSONObject.quote(UNUSUAL_STRING_EID),
          JSONObject.quote(UNUSUAL_STRING_EID),
          Integer.MIN_VALUE,
          Long.MAX_VALUE);
      final EntityId originalEid = ToolUtils.createEntityIdFromUserInputs(eidString, layout);
      final FijiRestEntityId restEid1 = FijiRestEntityId.create(originalEid, layout);
      final FijiRestEntityId restEid2 = FijiRestEntityId.createFromUrl(eidString, layout);
      final FijiRestEntityId restEid3 = FijiRestEntityId.create(
          String.format("hbase_hex=%s",
              new String(Hex.encodeHex((originalEid.getHBaseRowKey())))));
      final FijiRestEntityId restEid4 = FijiRestEntityId.create(
              String.format("hbase=%s", Bytes.toStringBinary(originalEid.getHBaseRowKey())));
      final EntityId toolUtilsEid = ToolUtils.createEntityIdFromUserInputs(eidString, layout);

      // Resolved entity id should match origin entity id.
      assertEquals(originalEid, restEid1.resolve(layout));
      assertEquals(originalEid, restEid2.resolve(layout));
      assertEquals(originalEid, restEid3.resolve(layout));
      assertEquals(originalEid, restEid4.resolve(layout));
      assertEquals(toolUtilsEid, restEid4.resolve(layout));
    }

    @Test
    public void testShouldCreateListsOfEntityIds() throws Exception {
      final TableLayoutDesc desc =
        FijiTableLayouts.getLayout("com.moz.fiji/rest/layouts/rkf_hashprefixed.json");
      final FijiTableLayout layout = FijiTableLayout.newLayout(desc);
      final EntityIdFactory factory = EntityIdFactory.getFactory(layout);
      final byte[] rowKey = Bytes.toBytes(UNUSUAL_STRING_EID);
      final EntityId originalEid = factory.getEntityIdFromHBaseRowKey(rowKey);

      // test the creation of entity ids from raw hbase rowkey
      final FijiRestEntityId restEid1 = FijiRestEntityId.createFromUrl(
          String.format("hbase_hex=%s", new String(Hex.encodeHex(originalEid.getHBaseRowKey()))),
          layout);
      final FijiRestEntityId restEid2 = FijiRestEntityId.createFromUrl(
          String.format("hbase=%s", Bytes.toStringBinary(originalEid.getHBaseRowKey())),
          layout);

      final JsonNodeFactory jsonNodeFactory = new JsonNodeFactory(true);
      final JsonNode hbaseHexStringNode = jsonNodeFactory.textNode(
          String.format("hbase_hex=%s", new String(Hex.encodeHex(originalEid.getHBaseRowKey()))));
      final JsonNode hbaseBinaryStringNode = jsonNodeFactory.textNode(
          String.format("hbase_hex=%s", new String(Hex.encodeHex(originalEid.getHBaseRowKey()))));
      ArrayNode hbaseListNode = jsonNodeFactory.arrayNode();
      hbaseListNode.add(hbaseHexStringNode);
      hbaseListNode.add(hbaseBinaryStringNode);

      final List<FijiRestEntityId> restEidList1 = FijiRestEntityId.createListFromUrl(
          hbaseListNode.toString(), layout);

      assertEquals(restEid1.resolve(layout), restEidList1.get(0).resolve(layout));
      assertEquals(restEid2.resolve(layout), restEidList1.get(1).resolve(layout));

      // test the creation of entity ids from various json strings
      final FijiRestEntityId restEid3 = FijiRestEntityId.createFromUrl(
          "[\"Hello\",\"World\"]", layout);
      final List<FijiRestEntityId> restEidList3 = FijiRestEntityId.createListFromUrl(
          "[[\"Hello\",\"World\"]]", layout);
      final FijiRestEntityId restEid4 = FijiRestEntityId.createFromUrl(
          "[[],\"World\"]", layout);
      final List<FijiRestEntityId> restEidList4 = FijiRestEntityId.createListFromUrl(
          "[[[],\"World\"],[\"Hello\",\"World\"]]", layout);

      assertEquals(restEid3.resolve(layout), restEidList3.get(0).resolve(layout));
      assertEquals(restEid4.getStringEntityId(), restEidList4.get(0).getStringEntityId());
      assertEquals(1, restEidList3.size());
      assertEquals(2, restEidList4.size());
    }

    @Test
    public void testIntegerComponentsShouldBePromotableToLong() throws Exception {
      final TableLayoutDesc desc =
          FijiTableLayouts.getLayout("com.moz.fiji/rest/layouts/rkf2_suppressed.json");
      final FijiTableLayout layout = FijiTableLayout.newLayout(desc);

      // Construct complex entity id.
      final String eidString = String.format("[%s,%s,%s,%d,%d]",
          JSONObject.quote(UNUSUAL_STRING_EID),
          JSONObject.quote(UNUSUAL_STRING_EID),
          JSONObject.quote(UNUSUAL_STRING_EID),
          0,
          0); // Promote this component.
      final FijiRestEntityId restEid = FijiRestEntityId.createFromUrl(eidString, layout);
      assertTrue(restEid.getComponents()[4] instanceof Long);
    }

    @Test
    public void testLongComponentsShouldNotComplain() throws Exception {
      final TableLayoutDesc desc =
          FijiTableLayouts.getLayout("com.moz.fiji/rest/layouts/rkf2_suppressed.json");
      final FijiTableLayout layout = FijiTableLayout.newLayout(desc);

      // Construct complex entity id.
      final String eidString = String.format("[%s,%s,%s,%d,%d]",
          JSONObject.quote(UNUSUAL_STRING_EID),
          JSONObject.quote(UNUSUAL_STRING_EID),
          JSONObject.quote(UNUSUAL_STRING_EID),
          0,
          Long.MAX_VALUE); // Long component of interest.
      final FijiRestEntityId restEid = FijiRestEntityId.createFromUrl(eidString, layout);
      assertTrue(restEid.getComponents()[4] instanceof Long);
    }
}

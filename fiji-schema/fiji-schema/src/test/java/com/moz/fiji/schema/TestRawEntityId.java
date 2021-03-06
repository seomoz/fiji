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

import org.junit.Test;

/** Tests for RawEntityId. */
public class TestRawEntityId {
  @Test
  public void testRawEntityId() {
    final byte[] bytes = new byte[] {0x11, 0x22};
    final RawEntityId eid = RawEntityId.getEntityId(bytes);
    assertArrayEquals(bytes, (byte[])eid.getComponentByIndex(0));
    assertArrayEquals(bytes, eid.getHBaseRowKey());
    assertEquals(1, eid.getComponents().size());
    assertEquals(bytes, eid.getComponents().get(0));
  }
}

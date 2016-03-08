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
import static junit.framework.Assert.assertNotNull;

import java.io.IOException;

import org.apache.hadoop.hbase.filter.RandomRowFilter;
import org.codehaus.jackson.JsonNode;
import org.junit.Test;

/**
 * Tests for {@link FijiRandomRowFilter}.
 */
public class TestFijiRandomRowFilter {

  @Test
  public void testHbaseFilterPropertiesSet() throws IOException {
    FijiRandomRowFilter rowFilter1 = new FijiRandomRowFilter(0.01F);
    RandomRowFilter filter = (RandomRowFilter) rowFilter1.toHBaseFilter(null);

    assertNotNull(filter);
    assertEquals(filter.getChance(), rowFilter1.getChance());
  }

  @Test
  public void testSerialization() throws Exception {
    FijiRowFilter rowFilter1 = getBaseFijiRowFilter();
    JsonNode serialized = rowFilter1.toJson();
    FijiRowFilter deserialized = FijiRowFilter.toFilter(serialized.toString());
    assertEquals(rowFilter1, deserialized);
  }

  @Test
  public void testHashCodeAndEquals() {
    FijiRowFilter rowFilter1 = getBaseFijiRowFilter();
    FijiRowFilter rowFilter2 = getBaseFijiRowFilter();

    assertEquals(rowFilter1.hashCode(), rowFilter2.hashCode());
    assertEquals(rowFilter1, rowFilter2);
  }

  public FijiRowFilter getBaseFijiRowFilter() {
    return new FijiRandomRowFilter(0.01F);
  }
}

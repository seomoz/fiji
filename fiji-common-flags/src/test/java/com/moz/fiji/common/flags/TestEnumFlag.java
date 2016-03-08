/**
 * Licensed to WibiData, Inc. under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  WibiData, Inc.
 * licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.moz.fiji.common.flags;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tests the flag parser for enumeration values. */
public class TestEnumFlag {
  private static final Logger LOG = LoggerFactory.getLogger(TestEnumFlag.class);

  public static enum Values {
    VALUE_ONE,
    VALUE_TWO,
    VALUE_THREE
  }

  private static final class TestObject {
    @Flag public Values flagEnum = null;
  }

  private TestObject mTest = new TestObject();

  @Test
  public void testEnumUnset() {
    FlagParser.init(mTest, new String[]{});
    assertNull(mTest.flagEnum);
  }

  @Test
  public void testEnumValueOne() {
    FlagParser.init(mTest, new String[]{"--flagEnum=VALUE_ONE"});
    assertEquals(Values.VALUE_ONE, mTest.flagEnum);
  }

  @Test
  public void testEnumValueOneLowerCase() {
    FlagParser.init(mTest, new String[]{"--flagEnum=value_one"});
    assertEquals(Values.VALUE_ONE, mTest.flagEnum);
  }

  @Test
  public void testEnumValueTwo() {
    FlagParser.init(mTest, new String[]{"--flagEnum=VALUE_TWO"});
    assertEquals(Values.VALUE_TWO, mTest.flagEnum);
  }

  @Test
  public void testEnumUnknown() {
    try {
      FlagParser.init(mTest, new String[]{"--flagEnum=unknown"});
      fail();
    } catch (IllegalFlagValueException ifve) {
      LOG.debug("Expected exception: {}", ifve.getMessage());
      assertEquals(
          "Invalid Values enum command-line argument '--flagEnum=unknown': "
          + "expecting one of VALUE_ONE,VALUE_TWO,VALUE_THREE.",
          ifve.getMessage());
    }
  }
}

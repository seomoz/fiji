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

package com.moz.fiji.schema.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class TestFijiNameValidator {
  @Test
  public void testIsValidIdentifier() {
    assertFalse(FijiNameValidator.isValidLayoutName(""));
    assertFalse(FijiNameValidator.isValidLayoutName("0123"));
    assertFalse(FijiNameValidator.isValidLayoutName("0123abc"));
    assertFalse(FijiNameValidator.isValidLayoutName("-asdb"));
    assertFalse(FijiNameValidator.isValidLayoutName("abc-def"));
    assertFalse(FijiNameValidator.isValidLayoutName("abcdef$"));
    assertFalse(FijiNameValidator.isValidLayoutName("abcdef-"));
    assertFalse(FijiNameValidator.isValidLayoutName("abcdef("));
    assertFalse(FijiNameValidator.isValidLayoutName("(bcdef"));
    assertFalse(FijiNameValidator.isValidLayoutName("(bcdef)"));

    assertTrue(FijiNameValidator.isValidLayoutName("_"));
    assertTrue(FijiNameValidator.isValidLayoutName("foo"));
    assertTrue(FijiNameValidator.isValidLayoutName("FOO"));
    assertTrue(FijiNameValidator.isValidLayoutName("FooBar"));
    assertTrue(FijiNameValidator.isValidLayoutName("foo123"));
    assertTrue(FijiNameValidator.isValidLayoutName("foo_bar"));

    assertFalse(FijiNameValidator.isValidLayoutName("abc:def"));
    assertFalse(FijiNameValidator.isValidLayoutName("abc\\def"));
    assertFalse(FijiNameValidator.isValidLayoutName("abc/def"));
    assertFalse(FijiNameValidator.isValidLayoutName("abc=def"));
    assertFalse(FijiNameValidator.isValidLayoutName("abc+def"));
  }

  @Test
  public void testIsValidAlias() {
    assertFalse(FijiNameValidator.isValidAlias(""));
    assertTrue(FijiNameValidator.isValidAlias("0123")); // Digits are ok in a leading capacity.
    assertTrue(FijiNameValidator.isValidAlias("0123abc"));
    assertTrue(FijiNameValidator.isValidAlias("abc-def")); // Dashes are ok in aliases...
    assertTrue(FijiNameValidator.isValidAlias("-asdb")); // Even as the first character.
    assertTrue(FijiNameValidator.isValidAlias("asdb-")); // Even as the first character.
    assertFalse(FijiNameValidator.isValidAlias("abcdef("));
    assertFalse(FijiNameValidator.isValidAlias("(bcdef"));
    assertFalse(FijiNameValidator.isValidAlias("(bcdef)"));

    assertTrue(FijiNameValidator.isValidAlias("_"));
    assertTrue(FijiNameValidator.isValidAlias("foo"));
    assertTrue(FijiNameValidator.isValidAlias("FOO"));
    assertTrue(FijiNameValidator.isValidAlias("FooBar"));
    assertTrue(FijiNameValidator.isValidAlias("foo123"));
    assertTrue(FijiNameValidator.isValidAlias("foo_bar"));

    assertFalse(FijiNameValidator.isValidAlias("abc:def"));
    assertFalse(FijiNameValidator.isValidAlias("abc\\def"));
    assertFalse(FijiNameValidator.isValidAlias("abc/def"));
    assertFalse(FijiNameValidator.isValidAlias("abc=def"));
    assertFalse(FijiNameValidator.isValidAlias("abc+def"));
  }

  @Test
  public void testIsValidInstanceName() {
    assertFalse(FijiNameValidator.isValidFijiName("")); // empty string is disallowed here.
    assertTrue(FijiNameValidator.isValidFijiName("0123")); // leading digits are okay.
    assertTrue(FijiNameValidator.isValidFijiName("0123abc")); // leading digits are okay.
    assertFalse(FijiNameValidator.isValidFijiName("-asdb"));
    assertFalse(FijiNameValidator.isValidFijiName("abc-def"));
    assertFalse(FijiNameValidator.isValidFijiName("abcd-"));
    assertFalse(FijiNameValidator.isValidFijiName("abcd$"));

    assertTrue(FijiNameValidator.isValidFijiName("_"));
    assertTrue(FijiNameValidator.isValidFijiName("foo"));
    assertTrue(FijiNameValidator.isValidFijiName("FOO"));
    assertTrue(FijiNameValidator.isValidFijiName("FooBar"));
    assertTrue(FijiNameValidator.isValidFijiName("foo123"));
    assertTrue(FijiNameValidator.isValidFijiName("foo_bar"));

    assertFalse(FijiNameValidator.isValidFijiName("abc:def"));
    assertFalse(FijiNameValidator.isValidFijiName("abc\\def"));
    assertFalse(FijiNameValidator.isValidFijiName("abc/def"));
    assertFalse(FijiNameValidator.isValidFijiName("abc=def"));
    assertFalse(FijiNameValidator.isValidFijiName("abc+def"));
  }
}

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

package com.moz.fiji.common.flags.parser;

import com.moz.fiji.common.flags.FlagSpec;
import com.moz.fiji.common.flags.IllegalFlagValueException;

/**
 * Parser for command-line flag parameters of native type boolean.
 *
 * Notes:
 * <ul>
 *   <li> flag evaluates to true if and only if the string value is "true" or "yes"
 *        (case insensitive match).</li>
 *   <li> flag evaluates to false if and only if the string value is "false" or "no"
 *        (case insensitive match).</li>
 *   <li> <code>"--flag"</code> is evaluated as <code>"--flag=true"</code></li>
 *   <li> <code>"--flag="</code> is evaluated as <code>"--flag=true"</code></li>
 *   <li> Other inputs are rejected.</li>
 * </ul>
 */
public class PrimitiveBooleanParser extends SimpleValueParser<Boolean> {
  /** {@inheritDoc} */
  @Override
  public Class<? extends Boolean> getParsedClass() {
    return boolean.class;
  }

  /** {@inheritDoc} */
  @Override
  public Boolean parse(FlagSpec flag, String string) {
    if (string == null) {
      // Handle the case: "--flag" without an equal sign:
      return true;
    }
    if (string.isEmpty()) {
      return true;
    }

    try {
      return Truth.parse(string);
    } catch (IllegalArgumentException iae) {
      throw new IllegalFlagValueException(flag, string);
    }
  }
}

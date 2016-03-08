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

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.google.common.collect.Maps;

import com.moz.fiji.common.flags.FlagSpec;
import com.moz.fiji.common.flags.IllegalFlagValueException;
import com.moz.fiji.common.flags.ValueParser;

/**
 * Parser for a list of strings.
 *
 * Usage example:
 *   <code>tool --flag=key1=value1 --flag=key2=value2 --value=key3=value3</code>
 */
public class StringMapParser implements ValueParser<Map> {
  /** {@inheritDoc} */
  @Override
  public Class<? extends Map> getParsedClass() {
    return Map.class;
  }

  /** {@inheritDoc} */
  @Override
  public boolean parsesSubclasses() {
    return true;
  }


  /** {@inheritDoc} */
  @Override
  public Map<String, String> parse(FlagSpec flag, List<String> values) {
    final TreeMap<String, String> map = Maps.newTreeMap();
    for (String keyValue : values) {
      if (keyValue == null) {
        throw new IllegalFlagValueException(flag, keyValue);
      }
      final String[] split = keyValue.split("=", 2);
      if (split.length != 2) {
        throw new IllegalFlagValueException(flag, keyValue);
      }
      map.put(split[0], split[1]);
    }
    return map;
  }
}
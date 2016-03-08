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

/**
 * Exception thrown when the type of a flag is not supported.
 *
 * This happens when annotating a field with an <pre>@Flag</pre> while there is no declared
 * parser for this field's type.
 */
public class UnsupportedFlagTypeException extends RuntimeException {
  /** Generated serial version ID. */
  private static final long serialVersionUID = -1500132821312834934L;

  /**
   * Creates a new <code>UnsupportedFlagTypeException</code> instance.
   *
   * @param spec Flag descriptor to create an "unsupported" exception for.
   */
  public UnsupportedFlagTypeException(FlagSpec spec) {
    super(String.format("Unsupported type '%s' for flag '--%s' declared in '%s'.",
        spec.getTypeName(), spec.getName(), spec.toString()));
  }
}

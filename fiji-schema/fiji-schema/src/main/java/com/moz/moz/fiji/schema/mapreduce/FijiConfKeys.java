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

package com.moz.fiji.schema.mapreduce;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;

/** Configuration keys used by FijiMR in Hadoop Configuration objects. */
@ApiAudience.Public
@ApiStability.Evolving
@Deprecated
public final class FijiConfKeys {

  /** URI of the input table to read from. */
  public static final String INPUT_TABLE_URI = "fiji.input.table.uri";

  /** URI of the output Fiji table to write to. */
  public static final String OUTPUT_KIJI_TABLE_URI = "fiji.output.table.uri";

  /** Serialized input data request. */
  public static final String INPUT_DATA_REQUEST = "fiji.input.request";

  /** Utility class may not be instantiated. */
  private FijiConfKeys() {
  }
}

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

import java.io.IOException;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;
import com.moz.fiji.annotations.Inheritance;

/**
 * Interface for Fiji cell encoders.
 *
 * A cell encoder is configured to encode precisely one specific column and knows exactly the type
 * of data it is supposed to encode.
 *
 * Cell encoders are instantiated via {@link FijiCellEncoderFactory}.
 *
 * Intended for framework developers only.
 */
@ApiAudience.Framework
@ApiStability.Evolving
@Inheritance.Sealed
public interface FijiCellEncoder {
  /**
   * Encodes the specified Fiji cell.
   *
   * @param cell Fiji cell to encode.
   * @return the binary encoding of the cell.
   * @throws IOException on I/O error.
   */
  byte[] encode(DecodedCell<?> cell) throws IOException;

  /**
   * Encodes the specified value.
   *
   * @param cellValue value to encode.
   * @return the binary encoding of the cell.
   * @throws IOException on I/O error.
   *
   * @param <T> type of the value to encode.
   */
  <T> byte[] encode(T cellValue) throws IOException;
}

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
 * Interface for Fiji cell decoders.
 *
 * A cell decoder is specific to one column and decodes only one type of value.
 * Cell decoders are instantiated via {@link FijiCellDecoderFactory}.
 *
 * @param <T> Type of the values being decoded.
 */
@ApiAudience.Framework
@ApiStability.Evolving
@Inheritance.Sealed
public interface FijiCellDecoder<T> {
  /**
   * Decodes a Fiji cell from its binary-encoded form.
   *
   * @param bytes Binary encoded Fiji cell.
   * @return the decoded FijiCell.
   * @throws IOException on I/O error.
   */
  DecodedCell<T> decodeCell(byte[] bytes) throws IOException;

  /**
   * Decodes a Fiji cell from its binary-encoded form.
   *
   * @param bytes Binary encoded Fiji cell value.
   * @return the decoded cell value.
   * @throws IOException on I/O error.
   */
  T decodeValue(byte[] bytes) throws IOException;
}

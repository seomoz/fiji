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
import com.moz.fiji.schema.impl.BoundColumnReaderSpec;
import com.moz.fiji.schema.layout.CellSpec;
import com.moz.fiji.schema.layout.FijiTableLayout;

/** Interface for factories of FijiCellDecoder instances. */
@ApiAudience.Framework
@ApiStability.Evolving
@Inheritance.Sealed
public interface FijiCellDecoderFactory {
  /**
   * Creates a new Fiji cell decoder.
   *
   * @param cellSpec Specification of the cell encoding.
   * @return a new Fiji cell decoder.
   * @throws IOException on I/O error.
   *
   * @param <T> Type of the value to decode.
   */
  <T> FijiCellDecoder<T> create(CellSpec cellSpec) throws IOException;

  /**
   * Creates a new Fiji cell decoder.
   *
   * @param layout FijiTableLayout from which to retrieve storage information.
   * @param spec Specification of the cell encoding.
   * @return a new Fiji cell decoder.
   * @throws IOException on I/O error.
   *
   * @param <T> Type of the value to decode.
   */
  <T> FijiCellDecoder<T> create(FijiTableLayout layout, BoundColumnReaderSpec spec)
      throws IOException;
}

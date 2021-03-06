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
import com.moz.fiji.schema.layout.CellSpec;

/** Interface for factories of FijiCellEncoder instances. */
@ApiAudience.Framework
@ApiStability.Evolving
@Inheritance.Sealed
public interface FijiCellEncoderFactory {
  /**
   * Creates a new Fiji cell encoder.
   *
   * @param cellSpec Specification of the cell encoding.
   * @return a new Fiji cell encoder.
   * @throws IOException on I/O error.
   */
  FijiCellEncoder create(CellSpec cellSpec) throws IOException;
}

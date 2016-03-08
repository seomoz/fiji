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

package com.moz.fiji.schema.impl;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.schema.layout.CellSpec;
import com.moz.fiji.schema.layout.FijiTableLayout;

/**
 * Decodes cells encoded using Avro into generic types.
 *
 * @param <T> The type of the decoded data.
 */
@ApiAudience.Private
public final class GenericCellDecoder<T> extends AvroCellDecoder<T> {
  /**
   * Initializes a cell decoder that creates generic Avro types.
   *
   * @param cellSpec Specification of the cell encoding.
   * @throws IOException on I/O error.
   */
  public GenericCellDecoder(CellSpec cellSpec) throws IOException {
    super(cellSpec);
  }

  /**
   * Initializes a cell decoder that creates generic Avro types.
   *
   * @param layout FijiTableLayout from which to get storage information.
   * @param spec Specification of the cell encoding.
   * @throws IOException on I/O error.
   */
  public GenericCellDecoder(FijiTableLayout layout, BoundColumnReaderSpec spec) throws IOException {
    super(layout, spec);
  }

  /** {@inheritDoc} */
  @Override
  protected DatumReader<T> createDatumReader(Schema writer, Schema reader) {
    return new GenericDatumReader<T>(writer, reader);
  }
}

/**
 * (c) Copyright 2013 WibiData, Inc.
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

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.schema.DecodedCell;
import com.moz.fiji.schema.FijiCellEncoder;
import com.moz.fiji.schema.avro.SchemaType;
import com.moz.fiji.schema.layout.CellSpec;

/**
 * Encoder for Fiji cells that expose raw bytes to the user.
 *
 * <p> Gives full control to the user about how a Fiji cell is encoded. </p>
 */
@ApiAudience.Private
public final class RawBytesCellEncoder implements FijiCellEncoder {
  private static final Logger LOG = LoggerFactory.getLogger(RawBytesCellEncoder.class);

  /** Specification of the column encoding. */
  private final CellSpec mCellSpec;

  /**
   * Initializes a new RawBytesCellEncoder.
   *
   * @param cellSpec Specification of the cell to encode.
   * @throws IOException on I/O error.
   */
  public RawBytesCellEncoder(final CellSpec cellSpec) throws IOException {
    mCellSpec = Preconditions.checkNotNull(cellSpec);
    Preconditions.checkArgument(cellSpec.getCellSchema().getType() == SchemaType.RAW_BYTES);
  }

  /** {@inheritDoc} */
  @Override
  public byte[] encode(final DecodedCell<?> cell) throws IOException {
    return encode(cell.getData());
  }

  /** {@inheritDoc} */
  @Override
  public synchronized <T> byte[] encode(final T cellValue) throws IOException {
    return (byte[]) cellValue;
  }

}

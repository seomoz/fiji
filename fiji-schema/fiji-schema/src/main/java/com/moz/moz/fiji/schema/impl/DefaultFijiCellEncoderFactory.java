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

import com.google.common.base.Preconditions;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.schema.FijiCellEncoder;
import com.moz.fiji.schema.FijiCellEncoderFactory;
import com.moz.fiji.schema.layout.CellSpec;

/** Factory for cell encoders. */
@ApiAudience.Private
public final class DefaultFijiCellEncoderFactory implements FijiCellEncoderFactory {
  /** Singleton instance. */
  private static final DefaultFijiCellEncoderFactory SINGLETON =
      new DefaultFijiCellEncoderFactory();

  /** @return the default factory for cell encoders. */
  public static DefaultFijiCellEncoderFactory get() {
    return SINGLETON;
  }

  /** Singleton constructor. */
  private DefaultFijiCellEncoderFactory() {
  }

  /** {@inheritDoc} */
  @Override
  public FijiCellEncoder create(final CellSpec cellSpec) throws IOException {
    Preconditions.checkNotNull(cellSpec);
    switch (cellSpec.getCellSchema().getType()) {
    case INLINE:
    case AVRO:
    case CLASS:
      return new AvroCellEncoder(cellSpec);
    case COUNTER:
      return CounterCellEncoder.get();
    case PROTOBUF:
      return new ProtobufCellEncoder(cellSpec);
    case RAW_BYTES:
      return new RawBytesCellEncoder(cellSpec);
    default:
      throw new RuntimeException("Unhandled cell encoding: " + cellSpec.getCellSchema().getType());
    }
  }
}

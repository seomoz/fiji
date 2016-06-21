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

import com.google.common.base.Preconditions;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;
import com.moz.fiji.schema.impl.BoundColumnReaderSpec;
import com.moz.fiji.schema.impl.CounterCellDecoder;
import com.moz.fiji.schema.impl.GenericCellDecoder;
import com.moz.fiji.schema.impl.ProtobufCellDecoder;
import com.moz.fiji.schema.impl.RawBytesCellDecoder;
import com.moz.fiji.schema.layout.CellSpec;
import com.moz.fiji.schema.layout.FijiTableLayout;

/**
 * Factory for Fiji cell decoders using GenericCellDecoder to handle record-based schemas.
 */
@ApiAudience.Framework
@ApiStability.Evolving
public final class GenericCellDecoderFactory implements FijiCellDecoderFactory {
  /** Singleton instance. */
  private static final GenericCellDecoderFactory SINGLETON = new GenericCellDecoderFactory();

  /** @return an instance of the GenericCellDecoderFactory. */
  public static FijiCellDecoderFactory get() {
    return SINGLETON;
  }

  /** Singleton constructor. */
  private GenericCellDecoderFactory() {
  }

  /** {@inheritDoc} */
  @Override
  public <T> FijiCellDecoder<T> create(CellSpec cellSpec) throws IOException {
    Preconditions.checkNotNull(cellSpec);
    switch (cellSpec.getCellSchema().getType()) {
      case AVRO:
      case CLASS:
      case INLINE:
        return new GenericCellDecoder<T>(cellSpec);
      case RAW_BYTES:
        return new RawBytesCellDecoder(cellSpec);
      case PROTOBUF:
        return new ProtobufCellDecoder<T>(cellSpec);
      case COUNTER:
        // purposefully forget the type (long) param of cell decoders for counters.
        @SuppressWarnings("unchecked")
        final FijiCellDecoder<T> counterCellDecoder = (FijiCellDecoder<T>) CounterCellDecoder.get();
        return counterCellDecoder;
      default:
        throw new InternalFijiError("Unhandled cell encoding: " + cellSpec.getCellSchema());
    }
  }

  /** {@inheritDoc} */
  @Override
  public <T> FijiCellDecoder<T> create(FijiTableLayout layout, BoundColumnReaderSpec spec)
      throws IOException {
    Preconditions.checkNotNull(layout);
    Preconditions.checkNotNull(spec);
    switch (spec.getColumnReaderSpec().getEncoding()) {
      case RAW_BYTES:
        return new RawBytesCellDecoder<T>(spec);
      case AVRO:
        return new GenericCellDecoder<T>(layout, spec);
      case PROTOBUF:
        return new ProtobufCellDecoder<T>(layout, spec);
      case COUNTER:
        // purposefully forget the type (long) param of cell decoders for counters.
        @SuppressWarnings("unchecked")
        final FijiCellDecoder<T> counterCellDecoder = (FijiCellDecoder<T>) CounterCellDecoder.get();
        return counterCellDecoder;
      default:
        throw new InternalFijiError(
            "Unhandled cell encoding in reader spec: " + spec.getColumnReaderSpec());
    }
  }
}

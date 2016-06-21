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
package com.moz.fiji.schema.layout.impl;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.schema.DecoderNotFoundException;
import com.moz.fiji.schema.GenericCellDecoderFactory;
import com.moz.fiji.schema.InternalFijiError;
import com.moz.fiji.schema.FijiCellDecoder;
import com.moz.fiji.schema.FijiCellDecoderFactory;
import com.moz.fiji.schema.FijiColumnName;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiDataRequest.Column;
import com.moz.fiji.schema.FijiSchemaTable;
import com.moz.fiji.schema.FijiTableReaderBuilder;
import com.moz.fiji.schema.FijiTableReaderBuilder.OnDecoderCacheMiss;
import com.moz.fiji.schema.SpecificCellDecoderFactory;
import com.moz.fiji.schema.impl.BoundColumnReaderSpec;
import com.moz.fiji.schema.layout.CellSpec;
import com.moz.fiji.schema.layout.ColumnReaderSpec;
import com.moz.fiji.schema.layout.ColumnReaderSpec.AvroDecoderType;
import com.moz.fiji.schema.layout.FijiTableLayout;

/**
 * Provider for cell decoders of a given table.
 *
 * <p>
 *   Cell decoders for all columns in the table are pro-actively created when the
 *   CellDecoderProvider is constructed.
 *   Cell decoders are cached and reused.
 * </p>
 * <p>
 *   At construction time, cell decoders may be customized by specifying BoundColumnReaderSpec
 *   instances to overlay on top of the actual table layout, using the constructor:
 *   {@link #create(FijiTableLayout, Map, Collection, FijiTableReaderBuilder.OnDecoderCacheMiss)}.
 * </p>
 * <p>
 *   CellSpec customizations include:
 * </p>
 *   <ul>
 *     <li> choosing between generic and specific Avro records. </li>
 *     <li> choosing a different Avro reader schema. </li>
 *     <li> using the Avro writer schema (this forces using generic records). </li>
 *   </ul>
 *
 * <h2>Thread Safety</h2>
 * <p>
 *   {@code CellDecoderProvider} is not thread safe.
 * </p>
 */
@ApiAudience.Private
public final class CellDecoderProvider {
  private static final Logger LOG = LoggerFactory.getLogger(CellDecoderProvider.class);

  /** Layout of the table for which decoders are provided. */
  private final FijiTableLayout mLayout;

  /** Cell decoders for columns, including data request and table reader overrides. */
  private final ImmutableMap<FijiColumnName, FijiCellDecoder<?>> mColumnDecoders;

  /**
   * Cell decoders for alternative columns specified in
   * {@link #create(FijiTableLayout, Map, Collection, FijiTableReaderBuilder.OnDecoderCacheMiss)}.
   */
  private final Map<BoundColumnReaderSpec, FijiCellDecoder<?>> mReaderSpecDecoders;

  /** Behavior when a decoder cannot be found. */
  private final OnDecoderCacheMiss mOnDecoderCacheMiss;

  /**
   * Constructor for {@code CellDecoderProvider}s.
   *
   * @param layout {@code FijiTableLayout} of the cell decoder provider's table.
   * @param columnDecoders Individual cell decoders for the columns of the table.
   * @param readerSpecDecoders Alternative cell decoders for columns of the table.
   * @param onDecoderCacheMiss Determines behavior when a decoder cannot be found.
   */
  private CellDecoderProvider(
      final FijiTableLayout layout,
      final ImmutableMap<FijiColumnName, FijiCellDecoder<?>> columnDecoders,
      final Map<BoundColumnReaderSpec, FijiCellDecoder<?>> readerSpecDecoders,
      final OnDecoderCacheMiss onDecoderCacheMiss
  ) {
    mLayout = layout;
    mColumnDecoders = columnDecoders;
    mReaderSpecDecoders = readerSpecDecoders;
    mOnDecoderCacheMiss = onDecoderCacheMiss;
  }

  /**
   * Initialize a provider for cell decoders.
   *
   * @param layout the layout for which to provide decoders.
   * @param schemaTable the schema table from which to retrieve cell schemas.
   * @param factory Default factory for cell decoders.
   * @param overrides Column specification overlay/override map.
   *     Specifications from this map override the actual specification from the table.
   * @throws IOException in case of an error creating the cached decoders.
   * @return a new {@code CellDecoderProvider}.
   */
  public static CellDecoderProvider create(
      final FijiTableLayout layout,
      final FijiSchemaTable schemaTable,
      final FijiCellDecoderFactory factory,
      final Map<FijiColumnName, CellSpec> overrides
  ) throws IOException {
    // Note: nothing prevents one from overriding the specification for one specific qualifier
    // in a map-type family.
    final Set<FijiColumnName> columns =
        Sets.newHashSet(Iterables.concat(overrides.keySet(), layout.getColumnNames()));

    // Pro-actively build cell decoders for all columns in the table:
    final ImmutableMap.Builder<FijiColumnName, FijiCellDecoder<?>> decoderMap =
        ImmutableMap.builder();
    for (FijiColumnName column : columns) {
      // Gets the specification for this column,
      // from the overlay map or else from the actual table layout:
      CellSpec cellSpec = overrides.get(column);
      if (null == cellSpec) {
        cellSpec = layout.getCellSpec(column);
      } else {
        // Deep-copy the user-provided CellSpec:
        cellSpec = CellSpec.copy(cellSpec);
      }

      // Fills in the missing details to build the decoder:
      if (cellSpec.getSchemaTable() == null) {
        cellSpec.setSchemaTable(schemaTable);
      }
      if (cellSpec.getDecoderFactory() == null) {
        cellSpec.setDecoderFactory(factory);
      }

      final FijiCellDecoder<?> decoder = cellSpec.getDecoderFactory().create(cellSpec);
      decoderMap.put(column, decoder);
    }
    return new CellDecoderProvider(
        layout,
        decoderMap.build(),
        Maps.<BoundColumnReaderSpec, FijiCellDecoder<?>>newHashMap(),
        FijiTableReaderBuilder.DEFAULT_CACHE_MISS);
  }

  /**
   * Create a provider for cell decoders.
   *
   * @param layout the layout for which to provide decoders.
   * @param overrides Column specification overlay/override map. Specifications from this map
   *     override the actual specification from the table.
   * @param onDecoderCacheMiss behavior to use when a decoder cannot be found.
   * @param alternatives alternate column specifications for which decoders should be provided.
   * @throws IOException in case of an error creating the cached decoders.
   * @return a new {@code CellDecoderProvider}.
   */
  public static CellDecoderProvider create(
      final FijiTableLayout layout,
      final Map<FijiColumnName, BoundColumnReaderSpec> overrides,
      final Collection<BoundColumnReaderSpec> alternatives,
      final OnDecoderCacheMiss onDecoderCacheMiss
  ) throws IOException {
    // Pro-actively build cell decoders for all columns in the table and spec overrides:
    return new CellDecoderProvider(
        layout,
        makeColumnDecoderMap(layout, overrides),
        makeSpecDecoderMap(layout, overrides.values(), alternatives),
        onDecoderCacheMiss);
  }

  /**
   * Create a new {@link FijiCellDecoder} from a {@link BoundColumnReaderSpec}.
   *
   * @param layout {@code FijiTableLayout} from which storage information will be retrieved to build
   *     decoders.
   * @param spec specification of column read properties from which to build a cell decoder.
   * @return a new cell decoder based on the specification.
   * @throws IOException in case of an error making the decoder.
   */
  private static FijiCellDecoder<?> createDecoderFromSpec(
      final FijiTableLayout layout,
      final BoundColumnReaderSpec spec
  ) throws IOException {
    final AvroDecoderType decoderType = spec.getColumnReaderSpec().getAvroDecoderType();
    if (null != decoderType) {
      switch (decoderType) {
        case GENERIC: {
          return GenericCellDecoderFactory.get().create(layout, spec);
        }
        case SPECIFIC: {
          return SpecificCellDecoderFactory.get().create(layout, spec);
        }
        default: throw new InternalFijiError("Unknown decoder type: " + decoderType);
      }
    } else {
      // If the decoder type is null, we can use the generic factory.
      return GenericCellDecoderFactory.get().create(layout, spec);
    }
  }

  /**
   * Build a map of {@link BoundColumnReaderSpec} to {@link FijiCellDecoder} from a collection of
   * specs.
   *
   * All columns in overrides and alternatives are assumed to be included in the table layout
   * because of prior validation.
   *
   * @param layout FijiTableLayout from which storage information will be retrieved to build
   *     decoders.
   * @param overrides specifications of column read properties from which to build decoders.
   * @param alternatives further specifications of column reader properties from which to build
   *     decoders.
   * @return a map from specification to decoders which follow those specifications.
   * @throws IOException in case of an error making decoders.
   */
  private static Map<BoundColumnReaderSpec, FijiCellDecoder<?>> makeSpecDecoderMap(
      final FijiTableLayout layout,
      final Collection<BoundColumnReaderSpec> overrides,
      final Collection<BoundColumnReaderSpec> alternatives
  ) throws IOException {
    final Map<BoundColumnReaderSpec, FijiCellDecoder<?>> decoderMap = Maps.newHashMap();
    for (BoundColumnReaderSpec spec : overrides) {
      Preconditions.checkState(null == decoderMap.put(spec, createDecoderFromSpec(layout, spec)));
    }
    for (BoundColumnReaderSpec spec : alternatives) {
      Preconditions.checkState(null == decoderMap.put(spec, createDecoderFromSpec(layout, spec)));
    }

    return decoderMap;
  }

  /**
   * Build a map of column names to {@link FijiCellDecoder} for all columns in a given layout and
   * set of overrides.
   *
   * All columns in overrides are assumed to be included in the table layout because of prior
   * validation.
   *
   * @param layout layout from which to get column names and column specifications.
   * @param overrides overridden column read properties.
   * @return a map from all columns in a table and overrides to decoders for those columns.
   * @throws IOException in case of an error making decoders.
   */
  private static ImmutableMap<FijiColumnName, FijiCellDecoder<?>> makeColumnDecoderMap(
      final FijiTableLayout layout,
      final Map<FijiColumnName, BoundColumnReaderSpec> overrides
  ) throws IOException {
    final Set<FijiColumnName> columns = layout.getColumnNames();
    final ImmutableMap.Builder<FijiColumnName, FijiCellDecoder<?>> decoderMap =
        ImmutableMap.builder();
    for (FijiColumnName column : columns) {
      // Gets the specification for this column,
      // from the overlay map or else from the actual table layout:
      final BoundColumnReaderSpec spec = overrides.get(column);
      if (null != spec) {
        decoderMap.put(column, createDecoderFromSpec(layout, spec));
      } else {
        final CellSpec cellSpec = layout.getCellSpec(column);
        decoderMap.put(column, cellSpec.getDecoderFactory().create(cellSpec));
      }
    }
    return decoderMap.build();
  }

  /**
   * Get a decoder from a {@link BoundColumnReaderSpec}. Creates a new decoder if one does not
   * already exist.
   *
   * @param spec specification of column read properties from which to get a decoder.
   * @param <T> the type of the value encoded in the cell.
   * @return a new or cached cell decoder corresponding to the given specification.
   * @throws IOException in case of an error create a new decoder.
   */
  @SuppressWarnings("unchecked")
  private <T> FijiCellDecoder<T> getDecoder(BoundColumnReaderSpec spec) throws IOException {
    final FijiCellDecoder<T> decoder = (FijiCellDecoder<T>) mReaderSpecDecoders.get(spec);
    if (null != decoder) {
      return decoder;
    } else {
      switch (mOnDecoderCacheMiss) {
        case FAIL: {
          throw new DecoderNotFoundException(
              "Could not find cell decoder for BoundColumnReaderSpec: " + spec);
        }
        case BUILD_AND_CACHE: {
          LOG.debug(
              "Building and caching new cell decoder from ColumnReaderSpec: {} for column: {}",
              spec.getColumnReaderSpec(), spec.getColumn());
          final FijiCellDecoder<T> newDecoder =
              (FijiCellDecoder<T>) createDecoderFromSpec(mLayout, spec);
          mReaderSpecDecoders.put(spec, newDecoder);
          return newDecoder;
        }
        case BUILD_DO_NOT_CACHE: {
          LOG.debug(
              "Building and not caching new cell decoder from ColumnReaderSpec: {} for column: {}",
              spec.getColumnReaderSpec(), spec.getColumn());
          return (FijiCellDecoder<T>) createDecoderFromSpec(mLayout, spec);
        }
        default: {
          throw new InternalFijiError("Unknown OnDecoderCacheMiss: " + mOnDecoderCacheMiss);
        }
      }
    }
  }

  // -----------------------------------------------------------------------------------------------
  // Public interface

  /**
   * Get a {@code CellDecoderProvider} with overrides applied from the provided request.
   *
   * @param request to overlay overrides from.
   * @return a {@code CellDecoderProvider} for the provided request.
   * @throws IOException on unrecoverable IO error.
   */
  public CellDecoderProvider getDecoderProviderForRequest(
      final FijiDataRequest request
  ) throws IOException {
    final List<BoundColumnReaderSpec> readerSpecs = Lists.newArrayList();
    for (Column columnRequest : request.getColumns()) {
      final ColumnReaderSpec readerSpec = columnRequest.getReaderSpec();
      if (readerSpec != null) {
        final FijiColumnName column = columnRequest.getColumnName();
        readerSpecs.add(BoundColumnReaderSpec.create(readerSpec, column));
      }
    }

    if (readerSpecs.isEmpty()) {
      return this;
    }

    final Map<FijiColumnName, FijiCellDecoder<?>> columnDecoders = Maps.newHashMap();
    columnDecoders.putAll(this.mColumnDecoders);

    for (BoundColumnReaderSpec readerSpec : readerSpecs) {
      columnDecoders.put(readerSpec.getColumn(), getDecoder(readerSpec));
    }

    return new CellDecoderProvider(
        mLayout,
        ImmutableMap.copyOf(columnDecoders),
        mReaderSpecDecoders,
        mOnDecoderCacheMiss);
  }

  /**
   * Gets a cell decoder for the specified column or (map-type) family.
   *
   * <p>
   *   When requesting a decoder for a column within a map-type family, the decoder for the
   *   entire map-type family will be returned unless an override has been specified for the
   *   exact fully-qualified column.
   * </p>
   *
   * @param column to look up.
   * @param <T> Type of the data to decode.
   * @return a cell decoder for the specified column. {@code null} if the column does not exist or
   *     if the family is not map-type.
   */
  @SuppressWarnings("unchecked")
  public <T> FijiCellDecoder<T> getDecoder(final FijiColumnName column) {
    final FijiCellDecoder<T> decoder = (FijiCellDecoder<T>) mColumnDecoders.get(column);
    if (decoder != null) {
      // There already exists a decoder for this column:
      return decoder;
    }

    if (column.isFullyQualified()) {
      // There is no decoder for the specified fully-qualified column.
      // Try the family (this will only work for map-type families):
      return getDecoder(FijiColumnName.create(column.getFamily(), null));
    }

    return null;
  }
}

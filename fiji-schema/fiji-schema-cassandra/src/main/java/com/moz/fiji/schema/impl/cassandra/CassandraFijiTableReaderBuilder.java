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
package com.moz.fiji.schema.impl.cassandra;

import java.io.IOException;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.schema.FijiColumnName;
import com.moz.fiji.schema.FijiTableReaderBuilder;
import com.moz.fiji.schema.layout.ColumnReaderSpec;

/** C* implementation of FijiTableReaderBuilder. */
@ApiAudience.Private
public final class CassandraFijiTableReaderBuilder implements FijiTableReaderBuilder {

  /**
   * Create a new CassandraFijiTableReaderBuilder for the given CassandraFijiTable.
   *
   * @param table CassandraFijiTable for which to build a reader.
   * @return a new CassandraFijiTableReaderBuilder.
   */
  public static CassandraFijiTableReaderBuilder create(
      final CassandraFijiTable table
  ) {
    return new CassandraFijiTableReaderBuilder(table);
  }

  private final CassandraFijiTable mTable;
  private OnDecoderCacheMiss mOnDecoderCacheMiss = null;
  private Map<FijiColumnName, ColumnReaderSpec> mOverrides = null;
  private Multimap<FijiColumnName, ColumnReaderSpec> mAlternatives = null;

  /**
   * Initialize a new CassandraFijiTableReaderBuilder for the given CassandraFijiTable.
   *
   * @param table CassandraFijiTable for which to build a reader.
   */
  private CassandraFijiTableReaderBuilder(
      final CassandraFijiTable table
  ) {
    mTable = table;
  }

  /** {@inheritDoc} */
  @Override
  public FijiTableReaderBuilder withOnDecoderCacheMiss(
      final OnDecoderCacheMiss behavior
  ) {
    Preconditions.checkNotNull(behavior, "OnDecoderCacheMiss behavior may not be null.");
    Preconditions.checkState(null == mOnDecoderCacheMiss,
        "OnDecoderCacheMiss behavior already set to: %s", mOnDecoderCacheMiss);
    mOnDecoderCacheMiss = behavior;
    return this;
  }

  /** {@inheritDoc} */
  @Override
  public OnDecoderCacheMiss getOnDecoderCacheMiss() {
    return mOnDecoderCacheMiss;
  }

  /** {@inheritDoc} */
  @Override
  public FijiTableReaderBuilder withColumnReaderSpecOverrides(
      final Map<FijiColumnName, ColumnReaderSpec> overrides
  ) {
    Preconditions.checkNotNull(overrides, "ColumnReaderSpec overrides may not be null.");
    Preconditions.checkState(null == mOverrides,
        "ColumnReaderSpec overrides already set to: %s", mOverrides);
    mOverrides = ImmutableMap.copyOf(overrides);
    return this;
  }

  /** {@inheritDoc} */
  @Override
  public Map<FijiColumnName, ColumnReaderSpec> getColumnReaderSpecOverrides() {
    return mOverrides;
  }

  /** {@inheritDoc} */
  @Override
  public FijiTableReaderBuilder withColumnReaderSpecAlternatives(
      final Multimap<FijiColumnName, ColumnReaderSpec> alternatives
  ) {
    Preconditions.checkNotNull(alternatives, "ColumnReaderSpec alternatives may not be null.");
    Preconditions.checkState(null == mAlternatives,
        "ColumnReaderSpec alternatives already set to: %s", mAlternatives);
    mAlternatives = ImmutableMultimap.copyOf(alternatives);
    return this;
  }

  /** {@inheritDoc} */
  @Override
  public Multimap<FijiColumnName, ColumnReaderSpec> getColumnReaderSpecAlternatives() {
    return mAlternatives;
  }

  /** {@inheritDoc} */
  @Override
  public CassandraFijiTableReader buildAndOpen() throws IOException {
    if (null == mOnDecoderCacheMiss) {
      mOnDecoderCacheMiss = DEFAULT_CACHE_MISS;
    }
    if (null == mOverrides) {
      mOverrides = DEFAULT_READER_SPEC_OVERRIDES;
    }
    if (null == mAlternatives) {
      mAlternatives = DEFAULT_READER_SPEC_ALTERNATIVES;
    }

    return CassandraFijiTableReader.createWithOptions(
        mTable, mOnDecoderCacheMiss, mOverrides, mAlternatives);
  }
}

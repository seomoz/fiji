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

package com.moz.fiji.mapreduce;

import java.io.IOException;

import com.google.common.base.Preconditions;
import org.apache.hadoop.io.NullWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;
import com.moz.fiji.annotations.Inheritance;
import com.moz.fiji.mapreduce.framework.HFileKeyValue;
import com.moz.fiji.mapreduce.impl.FijiTableContextFactory;

/**
 * Base class for reducers that emit to a Fiji table.
 *
 * Intended to be inherited by users to implement custom reducers writing to Fiji tables.
 *
 * @param <K> Type of the reducer input key.
 * @param <V> Type of the reducer input values.
 */
@ApiAudience.Public
@ApiStability.Stable
@Inheritance.Extensible
public abstract class FijiTableReducer<K, V>
    extends FijiReducer<K, V, HFileKeyValue, NullWritable> {
  private static final Logger LOG = LoggerFactory.getLogger(FijiTableReducer.class);

  /** Context used to emit to the output table. */
  private FijiTableContext mTableContext;

  /**
   * {@inheritDoc}
   * Sets up job resources.
   * User overridden setup methods must contain super.setup().  super.setup() should be the first
   * line of overridden setup methods so that so that KeyValueStores and FijiTableContext will be
   * initialized and ready for use by the rest of setup().
   */
  @Override
  protected void setup(Context hadoopContext) throws IOException, InterruptedException {
    Preconditions.checkState(mTableContext == null);
    super.setup(hadoopContext);

    mTableContext = FijiTableContextFactory.create(hadoopContext);
  }

  /** {@inheritDoc} */
  @Override
  protected final void reduce(K key, Iterable<V> values, Context hadoopContext)
      throws IOException, InterruptedException {
    // Implements the Hadoop reduce function:
    Preconditions.checkState(mTableContext != null, "KjiiTableContext is null because setup() "
        + "failed to execute.  If you overrode setup(), did you call super.setup()?");
    reduce(key, values, mTableContext);
  }

  /**
   * {@inheritDoc}
   * Cleans up job resources.
   * User overridden cleanup methods must contain super.cleanup().
   */
  @Override
  protected void cleanup(Context hadoopContext) throws IOException, InterruptedException {
    Preconditions.checkState(mTableContext != null);
    mTableContext.close();
    mTableContext = null;
    super.cleanup(hadoopContext);
  }

  /**
   * Fiji reducer function that processes the values for a key and emits to the output table.
   *
   * @param key Input key.
   * @param values Input values.
   * @param context Context to write to the configured output table.
   * @throws IOException on I/O error.
   */
  protected abstract void reduce(K key, Iterable<V> values, FijiTableContext context)
      throws IOException;

  /** {@inheritDoc} */
  @Override
  public final Class<?> getOutputKeyClass() {
    return HFileKeyValue.class;
  }

  /** {@inheritDoc} */
  @Override
  public final Class<?> getOutputValueClass() {
    return NullWritable.class;
  }
}

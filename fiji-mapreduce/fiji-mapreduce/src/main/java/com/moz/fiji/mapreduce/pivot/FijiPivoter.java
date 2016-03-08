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

package com.moz.fiji.mapreduce.pivot;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;
import com.moz.fiji.annotations.Inheritance;
import com.moz.fiji.mapreduce.FijiContext;
import com.moz.fiji.mapreduce.FijiTableContext;
import com.moz.fiji.mapreduce.kvstore.KeyValueStore;
import com.moz.fiji.mapreduce.kvstore.KeyValueStoreClient;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiRowData;

/**
 * Base class for a map-only job that reads from a Fiji table and writes to a Fiji table.
 *
 * <p>
 *   A {@link FijiPivoter} scans the rows from an input FijiTable and write cells into an
 *   output FijiTable. The input and the output Fiji tables may or may not be the same.
 *   Contrary to a {@link com.moz.fiji.mapreduce.produce.FijiProducer}, a {@link FijiPivoter} is free
 *   to write cells to any column of any row in the output table.
 * </p>
 *
 * <h1>Lifecycle:</h1>
 *
 * <p>
 * Instances are created using ReflectionUtils.
 * The {@link org.apache.hadoop.conf.Configuration} object is set immediately after
 * instantiation with a call to {@link #setConf(Configuration)}.
 * In order to initialize internal state before any other methods are called,
 * override the {@link #setConf(Configuration)} method.
 * </p>
 *
 * <p>
 * As a {@link KeyValueStoreClient}, FijiTableMapper has access to all
 * stores defined by {@link KeyValueStoreClient#getRequiredStores()}. Readers for
 * these stores are surfaced in the setup(), produce(), and cleanup() methods
 * via the Context provided to each by calling {@link FijiContext#getStore(String)}.
 * </p>
 *
 * <p>
 * Once the internal state is set, functions may be called in any order, except for
 * restrictions on setup(), produce(), and cleanup().
 * </p>
 *
 * <p>
 * setup() will get called once at the beginning of the map phase, followed by
 * a call to produce() for each input row.  Once all of these produce()
 * calls have completed, cleanup() will be called exactly once.  It is possible
 * that this setup-produce-cleanup cycle may repeat any number of times.
 * </p>
 *
 * <p>
 * A final guarantee is that setup(), produce(), and cleanup() will be called after
 * getDataRequest() and getOutputColumn() have each been called at least once.
 * </p>
 *
 * <h1>Skeleton:</h1>
 * <p>
 *   Any concrete implementation of a FijiTableMapper must implement
 *   {@link #getDataRequest()} and {@link #produce(FijiRowData, FijiTableContext)}.
 * </p>
 */
@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Extensible
public abstract class FijiPivoter
    implements Configurable, KeyValueStoreClient {

  /** Configuration of this pivoter. */
  private Configuration mConf;

  /**
   * All subclass of FijiPivoter must have a default constructor.
   * Constructors should be lightweight, since the framework is free to create new instances
   * at any time.
   */
  public FijiPivoter() {
  }

  /**
   * Sets the Configuration for this FijiPivoter to use.
   * This function is guaranteed to be called immediately after instantiation.
   * Override this method to initialize internal state from a configuration.
   *
   * <p>
   *   If you override this method, you must call <code>super.setConf();</code>
   *   or else the configuration will not be saved properly.
   * </p>
   *
   * @param conf Configuration to initialize this pivoter with.
   */
  @Override
  public void setConf(Configuration conf) {
    mConf = conf;
  }

  /** {@inheritDoc} */
  @Override
  public final Configuration getConf() {
    Preconditions.checkNotNull(mConf);
    return mConf;
  }

  /**
   * Returns a FijiDataRequest that describes which input columns need to be available to
   * the pivoter.
   *
   * <p> This method may be called multiple times, including before {@link #setup(FijiContext)}.
   * </p>
   *
   * @return a specification of the data requested by this pivoter.
   */
  public abstract FijiDataRequest getDataRequest();

  /** {@inheritDoc} */
  @Override
  public Map<String, KeyValueStore<?, ?>> getRequiredStores() {
    return Collections.emptyMap();
  }

  /**
   * Called once to initialize this pivoter before any calls to
   * {@link FijiPivoter#produce(FijiRowData, FijiTableContext)}.
   *
   * @param context The FijiContext providing access to KeyValueStores, Counters, etc.
   * @throws IOException on I/O error.
   */
  public void setup(FijiContext context) throws IOException {
    // By default, do nothing. Nothing may be added here, because subclasses may implement setup
    // methods without super.setup().
  }

  /**
   * Called to compute derived data for a single entity.  The input that is included is controlled
   * by the {@link com.moz.fiji.schema.FijiDataRequest} returned in {@link #getDataRequest}.
   *
   * @param row Input row from the input Fiji table, populated with the requested columns.
   * @param context Context used to write to the output Fiji table.
   * @throws IOException on I/O error.
   */
  public abstract void produce(FijiRowData row, FijiTableContext context) throws IOException;

  /**
   * Called once to clean up this pivoter after all
   * {@link #produce(FijiRowData, FijiTableContext)} calls are made.
   *
   * @param context The FijiContext providing access to KeyValueStores, Counters, etc.
   * @throws IOException on I/O error.
   */
  public void cleanup(FijiContext context) throws IOException {
    // By default, do nothing. Nothing may be added here, because subclasses may implement setup
    // methods without super.cleanup().
  }
}

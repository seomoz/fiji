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

package com.moz.fiji.mapreduce.produce;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;
import com.moz.fiji.annotations.Inheritance;
import com.moz.fiji.mapreduce.FijiContext;
import com.moz.fiji.mapreduce.kvstore.KeyValueStore;
import com.moz.fiji.mapreduce.kvstore.KeyValueStoreClient;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiRowData;

/**
 * <p>
 * Base class for all Fiji Producers used to generate per-row derived
 * entity data.
 * </p>
 *
 * <h1>Lifecycle:</h1>
 *
 * <p>
 * Instances are created using ReflectionUtils, so the {@link
 * org.apache.hadoop.conf.Configuration} is automagically set immediately after
 * instantiation with a call to setConf().  In order to initialize internal state
 * before any other methods are called, override the setConf() method.
 * </p>
 *
 * <p>
 * As a {@link KeyValueStoreClient}, FijiProducers will have access to all
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
 *   Any concrete implementation of a FijiProducer must implement the {@link #getDataRequest()},
 *   {@link #produce}, and {@link #getOutputColumn()} methods.
 *   An example of a produce method that extracts the domains from the email field of each row:
 * </p>
 * <pre><code>
 *   public void produce(FijiRowData input, ProducerContext context)
 *       throws IOException {
 *     if (!input.containsColumn("info", "email")) {
 *       return;
 *     }
 *     String email = input.getMostRecentValue("info", "email").toString();
 *     int atSymbol = email.indexOf("@");
 *
 *     String domain = email.substring(atSymbol + 1);
 *     context.put(domain);
 *   }
 * </code></pre>
 * For the entire code for this producer, check out EmailDomainProducer in FijiMR Lib.
 */
@ApiAudience.Public
@ApiStability.Stable
@Inheritance.Extensible
public abstract class FijiProducer
    implements Configurable, KeyValueStoreClient {

  /** The Configuration of this producer. */
  private Configuration mConf;

  /**
   * Your subclass of FijiProducer must have a default constructor if it is to be used in a
   * FijiProduceJob.  The constructors should be lightweight, since the framework is free to
   * create FijiProducers at any time.
   */
  public FijiProducer() {
  }

  /**
   * Sets the Configuration for this FijiProducer to use.
   * This function is guaranteed to be called immediately after instantiation.
   * Override this method to initialize internal state from a configuration.
   *
   * <p>If you override this method for your producer, you must call super.setConf(); or the
   * configuration will not be saved properly.</p>
   *
   * @param conf The Configuration to read.
   */
  @Override
  public void setConf(Configuration conf) {
    mConf = conf;
  }

  /**
   * {@inheritDoc}
   * <p>Overriding this method without returning super.getConf() may cause undesired behavior.</p>
   */
  @Override
  public Configuration getConf() {
    return mConf;
  }

  /**
   * Returns a FijiDataRequest that describes which input columns need to be available to
   * the producer.  This method may be called multiple times, perhaps before {@link
   * #setup(FijiContext)}.
   *
   * @return a fiji data request.
   */
  public abstract FijiDataRequest getDataRequest();

  /** {@inheritDoc} */
  @Override
  public Map<String, KeyValueStore<?, ?>> getRequiredStores() {
    return Collections.emptyMap();
  }

   /**
   * Return the name of the output column.  An output column is of the form "family" or
   * "family:qualifier".  Family columns can store key/value pairs.  A qualifier column
   * may only contain a single piece of data.
   *
   * @return the output column name.
   */
  public abstract String getOutputColumn();

  /**
   * Called once to initialize this producer before any calls to
   * {@link #produce(FijiRowData, ProducerContext)}.
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
   * @param input The requested input data for the entity.
   * @param context The producer context, used to output derived data.
   * @throws IOException on I/O error.
   */
  public abstract void produce(FijiRowData input, ProducerContext context) throws IOException;

  /**
   * Called once to clean up this producer after all
   * {@link #produce(FijiRowData, ProducerContext)} calls are made.
   *
   * @param context The FijiContext providing access to KeyValueStores, Counters, etc.
   * @throws IOException on I/O error.
   */
  public void cleanup(FijiContext context) throws IOException {
    // By default, do nothing. Nothing may be added here, because subclasses may implement setup
    // methods without super.cleanup().
  }
}

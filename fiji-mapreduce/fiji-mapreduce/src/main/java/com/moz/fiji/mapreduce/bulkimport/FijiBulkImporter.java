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

package com.moz.fiji.mapreduce.bulkimport;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;
import com.moz.fiji.annotations.Inheritance;
import com.moz.fiji.mapreduce.FijiTableContext;
import com.moz.fiji.mapreduce.kvstore.KeyValueStore;
import com.moz.fiji.mapreduce.kvstore.KeyValueStoreClient;

/**
 * <p>Base class for all Fiji bulk importers.  Subclasses of FijiBulkImporter can be
 * passed to the --importer flag of a <code>fiji bulk-import</code> command.</p>
 *
 * <p>To implement your own bulk importer, extend FijiBulkImporter and implement the
 * {@link #produce(Object, Object, FijiTableContext)} method to process
 * your input.  To write data to Fiji, call the appropriate <code>put()</code> method of the
 * {@link FijiTableContext}.</p>
 *
 * <h1>Lifecycle:</h1>
 *
 * <p>Internal state is set by a call to setConf().  Thus, FijiBulkImporters will be
 * automagically initialized by hadoop's ReflectionUtils.</p>
 *
 * <p>
 * As a {@link KeyValueStoreClient}, FijiBulkImporter will have access to all
 * stores defined by {@link KeyValueStoreClient#getRequiredStores()}. Readers for
 * these stores are surfaced in the setup(), produce(), and cleanup() methods
 * via the Context provided to each by calling
 * {@link com.moz.fiji.mapreduce.FijiContext#getStore(String)}.
 * </p>
 *
 * <p>Once the internal state is set, functions may be called in any order, except for
 * restrictions on setup(), produce(), and cleanup().</p>
 *
 * <p>setup() will get called once at the beginning of the map phase, followed by
 * a call to produce() for each input key-value pair.  Once all of these produce()
 * calls have completed, cleanup() will be called exactly once.  It is possible
 * that this setup-produce-cleanup cycle may repeat any number of times.</p>
 *
 * <p>A final guarantee is that setup(), produce(), and cleanup() will be called after
 * getOutputColumn() has been called at least once.</p>
 *
 * <h1>Skeleton:</h1>
 * <p>
 *   Any concrete implementation of a FijiBulkImporter must implement the {@link #produce} method.
 *   An example of a bulk importer that parses a colon delimited mappings of strings to integers:
 * </p>
 * <pre><code>
 *   public void produce(LongWritable filePos, Text value, FijiTableContext context)
 *       throws IOException {
 *     final String[] split = value.toString().split(":");
 *     final String rowKey = split[0];
 *     final int integerValue = Integer.parseInt(split[1]);

 *     final EntityId eid = context.getEntityId(rowKey);
 *     context.put(eid, "primitives", "int", integerValue);
 *   }
 * </code></pre>
 *
 * @param <K> The type of the MapReduce input key, which will depend on the input format used.
 * @param <V> The type of the MapReduce input value, which will depend on the input format used.
 */
@ApiAudience.Public
@ApiStability.Stable
@Inheritance.Extensible
public abstract class FijiBulkImporter<K, V>
    implements Configurable, KeyValueStoreClient {

  /** The Configuration of this producer. */
  private Configuration mConf;

  /**
   * Your subclass of FijiBulkImporter must have a default constructor if it is to be used
   * in a bulk import job.  The constructors should be lightweight, since the framework is
   * free to create FijiBulkImporters at any time.
   */
  public FijiBulkImporter() {
    mConf = new Configuration();
  }

  /**
   * {@inheritDoc}
   * <p>If you override this method for your bulk importer, you must call super.setConf(); or the
   * configuration will not be saved properly.</p>
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

  /** {@inheritDoc} */
  @Override
  public Map<String, KeyValueStore<?, ?>> getRequiredStores() {
    return Collections.emptyMap();
  }

  /**
   * Called once to initialize this bulk importer before any calls to
   * {@link #produce(Object, Object, FijiTableContext)}.
   *
   * @param context A context you can use to generate EntityIds and commit writes.
   *     See {@link FijiTableContext#getEntityId(Object...)}.
   * @throws IOException on I/O error.
   */
  public void setup(FijiTableContext context) throws IOException {
    // By default, do nothing. Nothing may be added here, because subclasses may implement setup
    // methods without super.setup().
  }

  /**
   * Produces data to be imported into Fiji.
   *
   * <p>Produce is called once for each key-value pair record from the raw input data.  To import
   * this data to Fiji, use the context.put(...) methods inherited from
   * {@link com.moz.fiji.schema.FijiPutter} to specify a cell address and value to write.  One
   * execution of produce may include multiple calls to put, which may span multiple rows, columns
   * and locality groups if desired. The context provides a
   * {@link FijiTableContext#getEntityId(Object...)} method for generating EntityIds from input
   * data.</p>
   *
   * @param key The MapReduce input key (its type depends on the InputFormat you use).
   * @param value The MapReduce input value (its type depends on the InputFormat you use).
   * @param context A context you can use to generate EntityIds and commit writes.
   *     See {@link FijiTableContext#getEntityId(Object...)} and
   *     {@link com.moz.fiji.schema.FijiPutter#put(com.moz.fiji.schema.EntityId, String, String, Object)}.
   * @throws IOException on I/O error.
   */
  public abstract void produce(K key, V value, FijiTableContext context)
      throws IOException;

  /**
   * Called once to clean up this bulk importer after all
   * {@link #produce(Object, Object, FijiTableContext)} calls are made.
   *
   * @param context A context you can use to generate EntityIds and commit writes.
   *     See {@link FijiTableContext#getEntityId(Object...)}.
   * @throws IOException on I/O error.
   */
  public void cleanup(FijiTableContext context) throws IOException {
    // By default, do nothing. Nothing may be added here, because subclasses may implement setup
    // methods without super.cleanup().
  }
}
